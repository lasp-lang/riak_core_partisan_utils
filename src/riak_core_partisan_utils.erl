%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Christopher Meiklejohn.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_core_partisan_utils).
-author("Christopher S. Meiklejohn <christopher.meiklejohn@gmail.com>").

-export([cast/3,
         bang_unreliable/3,
         bang_reliable/3,
         join/1, 
         leave/1, 
         update/1, 
         forward/4,
         configure_dispatch/0]).

%% The major difference in Riak was that bang vs. bang_unreliable were used
%% avoid buffer overflows with Distributed Erlang's distribution port;
%% bang_unreliable wouldn't establish a connection, where as the reliable
%% version would use gen_fsm:send; in our model, we have the function
%% distinction for portability but they both function mostly the same: until we need
%% the distinction.

cast(Channel, Destination, Message) ->
    Options = [noconnect, nosuspend],
    bang(Channel, Destination, {'$gen_cast', Message}, Options),
    ok.

bang_unreliable(Channel, Destination, Message) ->
    bang(Channel, Destination, Message, [noconnect, nosuspend]).

bang_reliable(Channel, Destination, Message) ->
    bang(Channel, Destination, Message, []).

bang(Channel, Pid, Message, _Options) when is_pid(Pid) ->
    Node = node(Pid),
    forward(Channel, Node, Pid, Message),
    Message;
bang(_Channel, Port, Message, Options) when is_port(Port) ->
    catch erlang:send(Port, Message, Options),
    Message;
bang(_Channel, RegName, Message, Options) when is_atom(RegName) ->
    catch erlang:send(RegName, Message, Options),
    Message;
bang(Channel, {RegName, Node}, Message, _Options) when is_atom(RegName) ->
    forward(Channel, Node, RegName, Message),
    Message;
bang(Channel, {partisan_remote_reference, Node, ProcessRef}, Message, _Options) ->
    forward(Channel, Node, ProcessRef, Message),
    Message;
bang(_Channel, Reference, _Message, _Options) ->
    exit({cannot_send_message, Reference}).

forward(Channel0, Peer, Module, Message) ->
    {ok, {Channel, Options}} = channel_and_options(Channel0),

    case should_dispatch() of
        false ->
            case node() of
                Peer ->
                    erlang:send(Module, Message);
                _ ->
                    case is_pid(Module) of
                        true ->
                            %% Can't remotely send to pid, only registered name; use proxy.
                            erlang:send({riak_core_partisan_proxy_service, Peer}, {forward, Module, Message}),
                            ok;
                        false ->
                            erlang:send({Module, Peer}, Message)
                    end
            end;
        true ->
            Manager = partisan_config:get(partisan_peer_service_manager,
                                          partisan_default_peer_service_manager),
            Manager:forward_message(Peer, Channel, Module, Message, Options)
    end.

update(Nodes) ->
    % lager:info("Membership now updating members to: ~p on node ~p", [Nodes, node()]),
    partisan_peer_service:update_members([node_map(Node) || Node <- Nodes]).

leave(Node) ->
    ok = partisan_peer_service:leave(Node).

join(Nodes) when is_list(Nodes) ->
    [join(Node) || Node <- Nodes],
    ok;
join(Node) ->
    lager:info("Starting join from partisan utils from ~p to ~p", [node(), Node]),

    %% Ignore failure, partisan will retry in the background to
    %% establish connections.
    ok = partisan_peer_service:sync_join(node_map(Node)),

    lager:info("Finishing join from ~p to ~p", [node(), Node]),

    ok.

node_map(Node) ->
    %% Use RPC to get the node's specific IP and port binding
    %% information for the partisan backend connections.
    ListenAddrs = rpc:call(Node, partisan_config, get, [listen_addrs]),

    %% Get parallelism...
    Parallelism = rpc:call(Node, partisan_config, get, [parallelism]),

    %% Get channels...
    Channels = rpc:call(Node, partisan_config, get, [channels]),

    #{name => Node, listen_addrs => ListenAddrs, channels => Channels, parallelism => Parallelism}.

%% @private
should_dispatch() ->
    partisan_mochiglobal:get(partisan_dispatch, false).

%% @private
configure_dispatch() ->
    ShouldDispatch = application:get_env(riak_core, partisan_dispatch, false),
    Dispatch = case ShouldDispatch of
        undefined ->
            false;
        Other ->
            Other
    end,
    %% Partisan itself has a short-circuit for message routing, if 
    %% the forward_message API is used directly.  Ensure the short-circuit
    %% is configured properly.
    case Dispatch of
        true ->
            partisan_config:set(disterl, false);
        false ->
            partisan_config:set(disterl, true)
    end,
    lager:info("Configuring partisan dispatch: ~p", [Dispatch]),
    partisan_mochiglobal:put(partisan_dispatch, Dispatch).

%% @private
channel_and_options(Channel) ->
    UseCausalDelivery = partisan_config:get(causal_delivery, false),

    case Channel of
        {vnode, Identifier} ->
            %% Use partition key routing based on vnode identifier.
            case UseCausalDelivery of
                true ->
                    {ok, {vnode, [{partition_key, Identifier}, {causal_label, vnode}]}};
                false ->
                    {ok, {vnode, [{partition_key, Identifier}]}}
            end;
        vnode ->
            %% Use the vnode channel.
            {ok, {vnode, []}};
        gossip ->
            %% Gossip should dispatch using a monotonic channel.
            {ok, {{monotonic, gossip}, []}};
        broadcast ->
            %% Use the broadcast channel.
            {ok, {broadcast, []}};
        Other ->
            lager:error("Unknown message type for partisan dispatch: ~p", [Other]),
            exit({error, {unknown_partisan_dispatch_type, Other}})
    end.
