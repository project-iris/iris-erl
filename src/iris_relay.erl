%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

%% @private

-module(iris_relay).
-export([connect/3, broadcast/3, request/4, reply/2, subscribe/2, publish/3,
	unsubscribe/2, tunnel/3, tunnel_send/3, tunnel_ack/2, tunnel_close/2, close/1]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
	code_change/3]).


%% =============================================================================
%% External API functions
%% =============================================================================

%% Starts the gen_server responsible for the relay connection.
-spec connect(Port :: pos_integer(), App :: string(), Handler :: pid()) ->
	{ok, Connection :: pid()} | {error, Reason :: atom()}.

connect(Port, App, Handler) ->
	gen_server:start(?MODULE, {Port, lists:flatten(App), Handler}, []).

%% Forwards a broadcasted message for relaying.
-spec broadcast(Connection :: pid(), App :: string(), Message :: binary()) ->
	ok | {error, Reason :: atom()}.

broadcast(Connection, App, Message) ->
	gen_server:call(Connection, {broadcast, lists:flatten(App), Message}, infinity).

%% Forwards the request to the relay. Timeouts are handled relay side.
-spec request(Connection :: pid(), App :: string(), Request :: binary(), Timeout :: pos_integer()) ->
	{ok, Reply :: binary()} | {error, Reason :: atom()}.

request(Connection, App, Request, Timeout) ->
	gen_server:call(Connection, {request, lists:flatten(App), Request, Timeout}, infinity).

%% Forwards an async reply to the relay to be sent back to the caller.
-spec reply(Sender :: iris:sender(), Reply :: binary()) ->
	ok | {error, Reason :: atom()}.

reply({Connection, RequestId}, Reply) ->
	gen_server:call(Connection, {reply, RequestId, Reply}, infinity).

%% Forwards the subscription request to the relay.
-spec subscribe(Connection :: pid(), Topic :: string()) ->
	ok | {error, Reason :: atom()}.

subscribe(Connection, Topic) ->
	gen_server:call(Connection, {subscribe, lists:flatten(Topic)}, infinity).

%% Publishes a message to the topic.
-spec publish(Connection :: pid(), Topic :: string(), Event :: binary()) ->
	ok | {error, Reason :: atom()}.

publish(Connection, Topic, Event) ->
	gen_server:call(Connection, {publish, lists:flatten(Topic), Event}, infinity).

%% Forwards the subscription removal request to the relay.
-spec unsubscribe(Connection :: pid(), Topic :: string()) ->
	ok | {error, Reason :: atom()}.

unsubscribe(Connection, Topic) ->
	gen_server:call(Connection, {unsubscribe, lists:flatten(Topic)}, infinity).

%% Forwards a tunneling request to the relay.
-spec tunnel(Connection :: pid(), App :: string(), Timeout :: pos_integer()) ->
	{ok, Tunnel :: iris:tunnel()} | {error, Reason :: atom()}.

tunnel(Connection, App, Timeout) ->
	gen_server:call(Connection, {tunnel, lists:flatten(App), Timeout}, infinity).

%% Forwards a tunnel data packet to the relay. Flow control should be already handled!
-spec tunnel_send(Connection :: pid(), TunId :: non_neg_integer(), Message :: binary()) ->
	ok | {error, Reason :: atom()}.

tunnel_send(Connection, TunId, Message) ->
	gen_server:call(Connection, {tunnel_send, TunId, Message}, infinity).

%% Forwards a tunnel data acknowledgement to the relay.
-spec tunnel_ack(Connection :: pid(), TunId :: non_neg_integer()) ->
	ok | {error, Reason :: atom()}.

tunnel_ack(Connection, TunId) ->
	gen_server:call(Connection, {tunnel_ack, TunId}, infinity).

%% Forwards a tunnel close request to the relay.
-spec tunnel_close(Connection :: pid(), TunId :: non_neg_integer()) ->
	ok | {error, Reason :: atom()}.

tunnel_close(Connection, TunId) ->
	gen_server:call(Connection, {tunnel_close, TunId}, infinity).

%% Notifies the relay server of a gracefull close request.
-spec close(Connection :: pid()) ->
	ok | {error, Reason :: atom()}.

close(Connection) ->
	gen_server:call(Connection, close, infinity).


%% =============================================================================
%% Generic server internal state
%% =============================================================================

-record(state, {
	sock,     %% Network connection to the iris node
	handler,  %% Handler for connection events

	reqIdx,   %% Index to assign the next request
	reqPend,  %% Active requests waiting for a reply

	subLive,  %% Active topic subscriptions

	tunIdx,   %% Index to assign the next tunnel
	tunPend,  %% Tunnels in the process of creation
	tunLive,  %% Active tunnels

	closer    %% Process requesting the relay closure
}).


%% =============================================================================
%% Generic server callback methods
%% =============================================================================

%% Connects to the locally running iris node and initiates the connection.
init({Port, App, Handler}) ->
	% Open the TCP connection
	case gen_tcp:connect({127,0,0,1}, Port, [{active, false}, binary, {nodelay, true}]) of
		{ok, Sock} ->
			% Send the init packet
			case iris_proto:sendInit(Sock, App) of
				ok ->
					% Wait for init confirmation
					case iris_proto:procInit(Sock) of
						ok ->
							% Spawn the receiver thread and return
							process_flag(trap_exit, true),
							spawn_link(iris_proto, process, [Sock, self(), Handler]),
							{ok, #state{
								sock    = Sock,
								handler = Handler,
								reqIdx  = 0,
								reqPend = ets:new(requests, [set, private]),
								subLive = ets:new(subscriptions, [set, private]),
								tunIdx  = 0,
								tunPend = ets:new(tunnels_pending, [set, private]),
								tunLive = ets:new(tunnels, [set, private]),
								closer  = nil
							}};
						{error, Reason} -> {stop, Reason}
					end;
				{error, Reason} -> {stop, Reason}
			end;
		{error, Reason} -> {stop, Reason}
	end.

%% Relays a message to the Iris node for broadcasting.
handle_call({broadcast, App, Message}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:sendBroadcast(Sock, App, Message), State};

%% Relays a request to the Iris node, waiting async for the reply.
handle_call({request, App, Request, Timeout}, From, State = #state{sock = Sock}) ->
	% Create a reply channel for the results
	ReqId = State#state.reqIdx,
	true = ets:insert_new(State#state.reqPend, {ReqId, From}),
	NewState = State#state{reqIdx = ReqId+1},

	% Send the request to the relay and finish with a pending reply
	case iris_proto:sendRequest(Sock, ReqId, App, Request, Timeout) of
		ok    -> {noreply, NewState};
		Error ->
			ets:delete(State#state.reqPend, ReqId),
			{reply, Error, NewState}
	end;

%% Relays a request reply to the Iris node.
handle_call({reply, ReqId, Reply}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:sendReply(Sock, ReqId, Reply), State};

%% Relays a subscription request to the Iris node (taking care of dupliactes).
handle_call({subscribe, Topic}, _From, State = #state{sock = Sock}) ->
	case ets:insert_new(State#state.subLive, {Topic}) of
		true  -> {reply, iris_proto:sendSubscribe(Sock, Topic), State};
		false -> {reply, {error, duplicate}}
	end;

%% Relays an event to the Iris node for topic publishing.
handle_call({publish, Topic, Event}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:sendPublish(Sock, Topic, Event), State};

%% Relays a subscription removal request to the Iris node (ensuring validity).
handle_call({unsubscribe, Topic}, _From, State = #state{sock = Sock}) ->
	% Make sure the subscription existed in the first hand
	case ets:member(State#state.subLive, Topic) of
		false -> {reply, {error, non_existent}};
		true ->
			ets:delete(State#state.subLive, Topic),
			{reply, iris_proto:sendUnsubscribe(Sock, Topic), State}
	end;

%% Sends a gracefull close request to the relay. The reply should arrive async.
handle_call(close, From, State = #state{sock = Sock}) ->
	case iris_proto:sendClose(Sock) of
		ok                      -> {noreply, State#state{closer = From}};
		Error = {error, Reason} -> {stop, Reason, Error, State}
	end;

%% Relays a tunneling request to the Iris node, waiting async with for the reply.
handle_call({tunnel, App, Timeout}, From, State = #state{sock = Sock}) ->
	% Create a result channel for the tunneling reply
	TunId = State#state.tunIdx,
	true = ets:insert_new(State#state.tunPend, {TunId, From}),
	NewState = State#state{tunIdx = TunId+1},

	% Send the request to the relay and finish with a pending reply
	case iris_proto:sendTunnelRequest(Sock, TunId, App, iris_tunnel:buffer(), Timeout) of
		ok    -> {noreply, NewState};
		Error ->
			ets:delete(State#state.tunPend, TunId),
			{reply, Error, NewState}
	end;

%% Relays a tunnel data packet to the Iris node.
handle_call({tunnel_send, TunId, Message}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:sendTunnelData(Sock, TunId, Message), State};

%% Relays a tunnel acknowledgement to the Iris node.
handle_call({tunnel_ack, TunId}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:sendTunnelAck(Sock, TunId), State};

%% Forwards a tunnel closing request to the relay if not yet closed remotely and
%% removes the tunnel from the local state.
handle_call({tunnel_close, TunId}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:sendTunnelClose(Sock, TunId), State}.

%% Delivers a reply to a pending request.
handle_info({reply, ReqId, Reply}, State) ->
	% Fetch the result channel and remove from state
	{ReqId, Pending} = hd(ets:lookup(State#state.reqPend, ReqId)),
	ets:delete(State#state.reqPend, ReqId),

	% Reply to the pending process and return
	gen_server:reply(Pending, Reply),
	{noreply, State};

%% Delivers a published event if the subscription is still alive.
handle_info({publish, Topic, Event}, State) ->
	% Make sure the subscription existed in the first hand
	case ets:member(State#state.subLive, Topic) of
		true  -> State#state.handler ! {publish, Topic, Event};
		false -> ok
	end,
	{noreply, State};

%% Accepts an incoming tunneling request from a remote app, assembling a local
%% tunnel with the given send window and replies to the relay with the final
%% permanent tunnel id.
handle_info({tunnel_request, TmpId, Buffer}, State = #state{sock = Sock}) ->
	% Create the local tunnel endpoint
	TunId = State#state.tunIdx,
	{ok, Tunnel} = gen_server:start(iris_tunnel, {self(), TunId, Buffer}, []),
	true = ets:insert_new(State#state.tunLive, {TunId, Tunnel}),

	% Acknowledge the tunnel creation to the relay
	ok = iris_proto:sendTunnelReply(Sock, TmpId, TunId, iris_tunnel:buffer()),

	% Notify the handler of the new tunnel
	State#state.handler ! {tunnel, {tunnel, Tunnel}},
	{noreply, State#state{tunIdx = TunId+1}};

% Delivers a reply to a pending tunneling request.
handle_info({tunnel_reply, TunId, Reply}, State) ->
	% Fetch the result channel and remove from state
	{TunId, Pending} = hd(ets:lookup(State#state.tunPend, TunId)),
	ets:delete(State#state.tunPend, TunId),

	% Reply to the pending process and return
	Result = case Reply of
		{ok, Buffer} ->
			case gen_server:start(iris_tunnel, {self(), TunId, Buffer}, []) of
				{ok, Tunnel} ->
					% Save the live tunnel
					true = ets:insert_new(State#state.tunLive, {TunId, Tunnel}),
					{ok, {tunnel, Tunnel}};
				Error -> Error
			end;
		Error -> Error
	end,
	gen_server:reply(Pending, Result),
	{noreply, State};

% Delivers a data packet to a specific tunnel.
handle_info({tunnel_data, TunId, Message}, State) ->
	{TunId, Tunnel} = hd(ets:lookup(State#state.tunLive, TunId)),
	Tunnel ! {data, Message},
	{noreply, State};

% Delivers a data acknowledgement to a specific tunnel.
handle_info({tunnel_ack, TunId}, State) ->
	case ets:lookup(State#state.tunLive, TunId) of
		[{TunId, Tunnel}] -> Tunnel ! ack;
		[]                -> ok
	end,
	{noreply, State};

%% Closes a tunnel connection, removing it from the local state.
handle_info({tunnel_close, TunId}, State) ->
	{TunId, Tunnel} = hd(ets:lookup(State#state.tunLive, TunId)),
  ets:delete(State#state.tunLive, TunId),

  Tunnel ! close,
  {noreply, State};

%% Handles the termination of the receiver thread: either returns a clean exit
%% or notifies the handler of a drop.
handle_info({'EXIT', _Pid, Reason}, State) ->
	% Notify all pending requests of the failure
	lists:foreach(fun({_ReqId, Pid}) ->
		gen_server:reply(Pid, {error, terminating})
	end, ets:tab2list(State#state.reqPend)),

	% Notify all tunnels of the closure
	lists:foreach(fun({_, Tunnel}) ->
		Tunnel ! close
	end, ets:tab2list(State#state.tunLive)),

	% Terminate, notifying either the closer or the handler
	case State#state.closer of
		nil ->
			State#state.handler ! {drop, Reason},
			{stop, Reason, State};
		Pid ->
			gen_server:reply(Pid, ok),
			{stop, normal, State}
	end.

%% Final cleanup, close up the relay link.
terminate(_Reason, State) ->
	gen_tcp:close(State#state.sock).


%% =============================================================================
%% Unused generic server methods
%% =============================================================================

code_change(_OldVsn, _State, _Extra) ->
	{error, unimplemented}.

handle_cast(_Request, State) ->
	{stop, unimplemented, State}.
