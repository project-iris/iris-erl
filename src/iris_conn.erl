%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% @private

-module(iris_conn).
-export([connect/1, register/3, broadcast/3, request/4, reply/2, subscribe/2, publish/3,
	unsubscribe/2, tunnel/3, tunnel_send/3, tunnel_ack/2, tunnel_close/2, close/1]).

-export([handle_broadcast/2, handle_request/5]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
	code_change/3]).


%% =============================================================================
%% External API functions
%% =============================================================================

%% Starts the gen_server responsible for a client connection.
-spec connect(Port :: pos_integer()) ->
	{ok, Connection :: pid()} | {error, Reason :: atom()}.

connect(Port) ->
	gen_server:start(?MODULE, {Port, "", nil}, []).


%% Starts the gen_server responsible for a service connection.
-spec register(Port :: pos_integer(), Cluster :: string(), Handler :: pid()) ->
  {ok, Connection :: pid()} | {error, Reason :: atom()}.

register(Port, Cluster, Handler) ->
  gen_server:start(?MODULE, {Port, lists:flatten(Cluster), Handler}, []).


%% Notifies the relay server of a graceful close request.
-spec close(Connection :: pid()) -> ok | {error, Reason :: term()}.

close(Connection) ->
  gen_server:call(Connection, close, infinity).


%% Forwards a broadcast message for relaying.
-spec broadcast(Connection :: pid(), Cluster :: string(), Message :: binary()) ->
	ok | {error, Reason :: atom()}.

broadcast(Connection, Cluster, Message) ->
	gen_server:call(Connection, {broadcast, lists:flatten(Cluster), Message}, infinity).


%% Forwards the request to the relay. Timeouts are handled relay side.
-spec request(Connection :: pid(), Cluster :: string(), Request :: binary(), Timeout :: pos_integer()) ->
	{ok, Reply :: binary()} | {error, Reason :: atom()}.

request(Connection, Cluster, Request, Timeout) ->
	gen_server:call(Connection, {request, lists:flatten(Cluster), Request, Timeout}, infinity).


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
-spec tunnel(Connection :: pid(), Cluster :: string(), Timeout :: pos_integer()) ->
	{ok, Tunnel :: iris:tunnel()} | {error, Reason :: atom()}.

tunnel(Connection, Cluster, Timeout) ->
	gen_server:call(Connection, {tunnel, lists:flatten(Cluster), Timeout}, infinity).


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


%% =============================================================================
%% Internal API callback functions
%% =============================================================================

%% Schedules an application broadcast for the service handler to process.
-spec handle_broadcast(Limiter :: pid(), Message :: binary()) -> ok.

handle_broadcast(Limiter, Message) ->
  ok = iris_mailbox:schedule(Limiter, byte_size(Message), {handle_broadcast, Message}).


%% Schedules an application request for the service handler to process.
-spec handle_request(Limiter :: pid(), Owner :: pid(), Id :: non_neg_integer(),
  Request :: binary(), Timeout :: pos_integer()) -> ok.

handle_request(Limiter, Owner, Id, Request, Timeout) ->
  ok = iris_mailbox:schedule(Limiter, byte_size(Request), {handle_request, {Owner, Id}, Request, Timeout}).


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
init({Port, Cluster, Handler}) ->
	% Open the TCP connection
	case gen_tcp:connect({127,0,0,1}, Port, [{active, false}, binary, {nodelay, true}]) of
		{ok, Sock} ->
			% Send the init packet
			case iris_proto:send_init(Sock, Cluster) of
				ok ->
					% Wait for init confirmation
					case iris_proto:proc_init(Sock) of
						{ok, _Version} ->
							% Spawn the mailbox limiter threads and message receiver
							process_flag(trap_exit, true),
              Broadcaster = iris_mailbox:start_link(Handler, broadcast, 64 * 1024),
              Requester   = iris_mailbox:start_link(Handler, request, 64 * 1024),
              _Processor  = iris_proto:start_link(Sock, Broadcaster, Requester),

              % Assemble the internal state and return
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

%% Sends a graceful close request to the relay. The reply will arrive async.
handle_call(close, From, State = #state{sock = Sock}) ->
  ok = iris_proto:send_close(Sock),
  {noreply, State#state{closer = From}};

%% Relays a message to the Iris node for broadcasting.
handle_call({broadcast, Cluster, Message}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:send_broadcast(Sock, Cluster, Message), State};

%% Relays a request to the Iris node, waiting async for the reply.
handle_call({request, Cluster, Request, Timeout}, From, State = #state{sock = Sock}) ->
	% Create a reply channel for the results
	ReqId = State#state.reqIdx,
	true = ets:insert_new(State#state.reqPend, {ReqId, From}),
	NewState = State#state{reqIdx = ReqId+1},

	% Send the request to the relay and finish with a pending reply
	case iris_proto:send_request(Sock, ReqId, Cluster, Request, Timeout) of
		ok    -> {noreply, NewState};
		Error ->
			ets:delete(State#state.reqPend, ReqId),
			{reply, Error, NewState}
	end;

%% Relays a request reply to the Iris node.
handle_call({reply, ReqId, Reply}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:send_reply(Sock, ReqId, Reply), State};

%% Relays a subscription request to the Iris node (taking care of dupliactes).
handle_call({subscribe, Topic}, _From, State = #state{sock = Sock}) ->
	case ets:insert_new(State#state.subLive, {Topic}) of
		true  -> {reply, iris_proto:send_subscribe(Sock, Topic), State};
		false -> {reply, {error, duplicate}}
	end;

%% Relays an event to the Iris node for topic publishing.
handle_call({publish, Topic, Event}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:send_publish(Sock, Topic, Event), State};

%% Relays a subscription removal request to the Iris node (ensuring validity).
handle_call({unsubscribe, Topic}, _From, State = #state{sock = Sock}) ->
	% Make sure the subscription existed in the first hand
	case ets:member(State#state.subLive, Topic) of
		false -> {reply, {error, non_existent}};
		true ->
			ets:delete(State#state.subLive, Topic),
			{reply, iris_proto:send_unsubscribe(Sock, Topic), State}
	end;

%% Relays a tunneling request to the Iris node, waiting async with for the reply.
handle_call({tunnel, Cluster, Timeout}, From, State = #state{sock = Sock}) ->
	% Create a result channel for the tunneling reply
	TunId = State#state.tunIdx,
	true = ets:insert_new(State#state.tunPend, {TunId, From}),
	NewState = State#state{tunIdx = TunId+1},

	% Send the request to the relay and finish with a pending reply
	case iris_proto:send_tunnel_request(Sock, TunId, Cluster, iris_tunnel:buffer(), Timeout) of
		ok    -> {noreply, NewState};
		Error ->
			ets:delete(State#state.tunPend, TunId),
			{reply, Error, NewState}
	end;

%% Relays a tunnel data packet to the Iris node.
handle_call({tunnel_send, TunId, Message}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:send_tunnel_data(Sock, TunId, Message), State};

%% Relays a tunnel acknowledgement to the Iris node.
handle_call({tunnel_ack, TunId}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:send_tunnel_ack(Sock, TunId), State};

%% Forwards a tunnel closing request to the relay if not yet closed remotely and
%% removes the tunnel from the local state.
handle_call({tunnel_close, TunId}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:send_tunnel_close(Sock, TunId), State}.

%% Delivers a reply to a pending request.
handle_cast({reply, ReqId, Reply}, State) ->
	% Fetch the result channel and remove from state
	{ReqId, Pending} = hd(ets:lookup(State#state.reqPend, ReqId)),
	ets:delete(State#state.reqPend, ReqId),

	% Reply to the pending process and return
	gen_server:reply(Pending, Reply),
	{noreply, State};

%% Delivers a published event if the subscription is still alive.
handle_cast({publish, Topic, Event}, State) ->
	% Make sure the subscription existed in the first hand
	case ets:member(State#state.subLive, Topic) of
		true  -> State#state.handler ! {publish, Topic, Event};
		false -> ok
	end,
	{noreply, State};

%% Accepts an incoming tunneling request from a remote app, assembling a local
%% tunnel with the given send window and replies to the relay with the final
%% permanent tunnel id.
handle_cast({tunnel_request, TmpId, Buffer}, State = #state{sock = Sock}) ->
	% Create the local tunnel endpoint
	TunId = State#state.tunIdx,
	{ok, Tunnel} = gen_server:start(iris_tunnel, {self(), TunId, Buffer}, []),
	true = ets:insert_new(State#state.tunLive, {TunId, Tunnel}),

	% Acknowledge the tunnel creation to the relay
	ok = iris_proto:send_tunnel_reply(Sock, TmpId, TunId, iris_tunnel:buffer()),

	% Notify the handler of the new tunnel
	State#state.handler ! {tunnel, {tunnel, Tunnel}},
	{noreply, State#state{tunIdx = TunId+1}};

% Delivers a reply to a pending tunneling request.
handle_cast({tunnel_reply, TunId, Reply}, State) ->
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
handle_cast({tunnel_data, TunId, Message}, State) ->
	{TunId, Tunnel} = hd(ets:lookup(State#state.tunLive, TunId)),
	Tunnel ! {data, Message},
	{noreply, State};

% Delivers a data acknowledgment to a specific tunnel.
handle_cast({tunnel_ack, TunId}, State) ->
	case ets:lookup(State#state.tunLive, TunId) of
		[{TunId, Tunnel}] -> Tunnel ! ack;
		[]                -> ok
	end,
	{noreply, State};

%% Closes a tunnel connection, removing it from the local state.
handle_cast({tunnel_close, TunId}, State) ->
	{TunId, Tunnel} = hd(ets:lookup(State#state.tunLive, TunId)),
  ets:delete(State#state.tunLive, TunId),

  Tunnel ! close,
  {noreply, State}.

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
