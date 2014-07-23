%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% @private

-module(iris_conn).
-export([connect/2, connect_link/2, register/5, register_link/5, close/1,
	broadcast/3, request/4, reply/2, subscribe/5, publish/3, unsubscribe/2,
	tunnel/3, tunnel_send/4, tunnel_allowance/3, tunnel_close/2, logger/1]).

-export([handle_reply/3, handle_tunnel_init/3, handle_tunnel_result/3,
	handle_tunnel_close/2]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
	code_change/3]).


%% =============================================================================
%% External API functions
%% =============================================================================

%% Starts the gen_server responsible for a client connection.
-spec connect(Port :: pos_integer(), Logger :: iris_logger:logger()) ->
	{ok, Connection :: pid()} | {error, Reason :: atom()}.

connect(Port, Logger) ->
	gen_server:start(?MODULE, {Port, "", nil, {0, 0}, Logger}, []).


%% Starts the gen_server responsible for a client connection and links it.
-spec connect_link(Port :: pos_integer(), Logger :: iris_logger:logger()) ->
	{ok, Connection :: pid()} | {error, Reason :: atom()}.

connect_link(Port, Logger) ->
	gen_server:start_link(?MODULE, {Port, "", nil, {0, 0}, Logger}, []).


%% Starts the gen_server responsible for a service connection.
-spec register(Port :: pos_integer(), Cluster :: string(), Handler :: pid(),
	Limits :: {pos_integer(), pos_integer()}, Logger :: iris_logger:logger()) ->
	{ok, Connection :: pid()} | {error, Reason :: atom()}.

register(Port, Cluster, Handler, Limits, Logger) ->
	gen_server:start(?MODULE, {Port, lists:flatten(Cluster), Handler, Limits, Logger}, []).


%% Starts the gen_server responsible for a service connection.
-spec register_link(Port :: pos_integer(), Cluster :: string(), Handler :: pid(),
	Limits :: {pos_integer(), pos_integer()}, Logger :: iris_logger:logger()) ->
	{ok, Connection :: pid()} | {error, Reason :: atom()}.

register_link(Port, Cluster, Handler, Limits, Logger) ->
	gen_server:start_link(?MODULE, {Port, lists:flatten(Cluster), Handler, Limits, Logger}, []).


%% Notifies the relay server of a graceful close request.
-spec close(Connection :: pid()) -> ok | {error, Reason :: term()}.

close(Connection) ->
  gen_server:call(Connection, close, infinity).


%% Forwards a broadcast message for relaying.
-spec broadcast(Connection :: pid(), Cluster :: string(), Message :: binary()) ->	ok.

broadcast(Connection, Cluster, Message) ->
	gen_server:call(Connection, {broadcast, lists:flatten(Cluster), Message}, infinity).


%% Forwards the request to the relay. Timeouts are handled relay side.
-spec request(Connection :: pid(), Cluster :: string(), Request :: binary(), Timeout :: pos_integer()) ->
	{ok, Reply :: binary()} | {error, Reason :: atom()}.

request(Connection, Cluster, Request, Timeout) ->
	gen_server:call(Connection, {request, lists:flatten(Cluster), Request, Timeout}, infinity).


%% Forwards an async reply to the relay to be sent back to the caller.
-spec reply(Sender :: iris:sender(), {ok, Reply :: binary()} | {error, Reason :: term()}) ->
	ok | {error, Reason :: atom()}.

reply({Connection, RequestId}, Response) ->
	gen_server:call(Connection, {reply, RequestId, Response}, infinity).


%% Forwards the subscription request to the relay.
-spec subscribe(Conn :: pid(), Topic :: string(), Module :: atom(), Args :: term(),
	Options :: [{atom(), term()}]) ->	ok | {error, Reason :: atom()}.

subscribe(Connection, Topic, Module, Args, Options) ->
	gen_server:call(Connection, {subscribe, lists:flatten(Topic), Module, Args, Options}, infinity).


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
	{ok, Tunnel :: pid()} | {error, Reason :: atom()}.

tunnel(Connection, Cluster, Timeout) ->
	gen_server:call(Connection, {tunnel, lists:flatten(Cluster), Timeout}, infinity).


%% Forwards a tunnel data packet to the relay. Flow control should be already handled!
-spec tunnel_send(Connection :: pid(), TunId :: non_neg_integer(), SizeOrCont :: non_neg_integer(),
	Message :: binary()) -> ok | {error, Reason :: atom()}.

tunnel_send(Connection, TunId, SizeOrCont, Message) ->
	gen_server:call(Connection, {tunnel_send, TunId, SizeOrCont, Message}, infinity).


%% Forwards a tunnel data acknowledgment to the relay.
-spec tunnel_allowance(Connection :: pid(), TunId :: non_neg_integer(),
	Allowance :: pos_integer()) -> ok | {error, Reason :: atom()}.

tunnel_allowance(Connection, TunId, Allowance) ->
	gen_server:call(Connection, {tunnel_allowance, TunId, Allowance}, infinity).


%% Forwards a tunnel close request to the relay.
-spec tunnel_close(Connection :: pid(), TunId :: non_neg_integer()) ->
	ok | {error, Reason :: atom()}.

tunnel_close(Connection, TunId) ->
	gen_server:call(Connection, {tunnel_close, TunId}, infinity).


logger(Connection) ->
	gen_server:call(Connection, {logger}, infinity).


%% =============================================================================
%% Internal API callback functions
%% =============================================================================

%% @private
%% Schedules an application broadcast for the service handler to process.
-spec handle_reply(Connection :: pid(), Id :: pos_integer(),
	{ok, Reply :: binary()} | {error, Reason :: term()}) -> ok.

handle_reply(Connection, Id, Response) ->
	gen_server:cast(Connection, {handle_reply, Id, Response}).


-spec handle_tunnel_init(Connection :: pid(), Id :: non_neg_integer(),
	ChunkLimit :: pos_integer()) -> ok.

handle_tunnel_init(Connection, Id, ChunkLimit) ->
	gen_server:cast(Connection, {handle_tunnel_init, Id, ChunkLimit}).


-spec handle_tunnel_result(Connection :: pid(), Id :: non_neg_integer(),
	Result :: {ok, ChunkLimit :: pos_integer} | {error, timeout}) -> ok.

handle_tunnel_result(Connection, Id, Result) ->
	gen_server:cast(Connection, {handle_tunnel_result, Id, Result}).


-spec handle_tunnel_close(Connection :: pid(), Id :: non_neg_integer()) -> ok.

handle_tunnel_close(Connection, Id) ->
	gen_server:cast(Connection, {handle_tunnel_close, Id}).


%% =============================================================================
%% Generic server internal state
%% =============================================================================

-record(state, {
	sock,     %% Network connection to the iris node
	procpid,  %% Network data reader process
	handler,  %% Handler for connection events

	reqIdx,   %% Index to assign the next request
	reqPend,  %% Active requests waiting for a reply

	subIdx,   %% Index to assign the next subscription (logging purposes)
	subLive,  %% Active topic subscriptions

	tunIdx,   %% Index to assign the next tunnel
	tunPend,  %% Tunnels in the process of creation
	tunLive,  %% Active tunnels

	closer,   %% Process requesting the relay closure
  logger    %% Logger with connection id injected
}).


%% =============================================================================
%% Generic server callback methods
%% =============================================================================

%% Connects to the locally running iris node and initiates the connection.
init({Port, Cluster, Handler, {BroadcastMemory, RequestMemory}, Logger}) ->
	% Open the TCP connection
	case gen_tcp:connect({127,0,0,1}, Port, [{active, false}, binary, {nodelay, true}]) of
		{ok, Sock} ->
			% Send the init packet
			case iris_proto:send_init(Sock, Cluster) of
				ok ->
					% Wait for init confirmation
					case iris_proto:proc_init(Sock) of
						{ok, _Version} ->
							Topics  = ets:new(subscriptions, [set, protected]),
							Tunnels = ets:new(tunnels, [set, protected]),

							% Spawn the mailbox limiter threads and message receiver
							process_flag(trap_exit, true),
							Broadcaster = iris_mailbox:start_link(Handler, BroadcastMemory, Logger),
							Requester   = iris_mailbox:start_link(Handler, RequestMemory, Logger),
							Processor   = iris_proto:start_link(Sock, Broadcaster, Requester, Topics, Tunnels),

							% Assemble the internal state and return
							{ok, #state{
								sock    = Sock,
								procpid = Processor,
								handler = Handler,
								reqIdx  = 0,
								reqPend = ets:new(requests, [set, private]),
								subIdx  = 1,
								subLive = Topics,
								tunIdx  = 0,
								tunPend = ets:new(tunnels_pending, [set, private]),
								tunLive = Tunnels,
								closer  = nil,
								logger  = Logger
							}};
						{error, Reason} -> {stop, Reason}
					end;
				{error, Reason} -> {stop, Reason}
			end;
		{error, Reason} -> {stop, Reason}
	end.

%% Sends a graceful close request to the relay. The reply will arrive async.
handle_call(close, From, State = #state{sock = Sock}) ->
	iris_logger:info(State#state.logger, "detaching from relay"),
	ok = iris_proto:send_close(Sock),
	{noreply, State#state{closer = From}};

%% Relays a message to the Iris node for broadcasting.
handle_call({broadcast, Cluster, Message}, _From, State = #state{sock = Sock}) ->
	iris_logger:debug(State#state.logger, "sending new broadcast", [{cluster, Cluster}, {data, Message}]),
	{reply, iris_proto:send_broadcast(Sock, Cluster, Message), State};

%% Relays a request to the Iris node, waiting async for the reply.
handle_call({request, Cluster, Request, Timeout}, From, State = #state{sock = Sock}) ->
	% Create a reply channel for the results
	ReqId = State#state.reqIdx,
	true = ets:insert_new(State#state.reqPend, {ReqId, From}),
	NewState = State#state{reqIdx = ReqId+1},

	% Send the request to the relay and finish with a pending reply
	iris_logger:debug(State#state.logger, "sending new request",
		[{local_request, ReqId}, {cluster, Cluster}, {data, Request}, {timeout, Timeout}]
	),
	ok = iris_proto:send_request(Sock, ReqId, Cluster, Request, Timeout),
	{noreply, NewState};

%% Relays a request reply to the Iris node.
handle_call({reply, ReqId, Response}, _From, State = #state{sock = Sock}) ->
  % Extract the response details and log it
	{Reply, Error} = case	Response of
		{ok, Rep}      -> {Rep, ""};
		{error, Fault} -> {<<>>, Fault}
	end,
	iris_logger:debug(State#state.logger, "replying to handled request",
		[{remote_request, ReqId}, {reply, Reply}, {error, Error}]
	),
	% Send it over the network
	{reply, iris_proto:send_reply(Sock, ReqId, Response), State};

%% Relays a subscription request to the Iris node (taking care of duplicates).
handle_call({subscribe, Topic, Module, Args, Options}, _From, State = #state{sock = Sock, subIdx = Idx}) ->
	% Make sure the subscription limits have valid values
	MemoryLimit = case proplists:lookup(event_memory, Options) of
		none                  -> iris_limits:default_topic_memory();
		{event_memory, Limit} -> Limit
	end,

	% Create a topic logger with the name injected
	Logger = iris_logger:new(State#state.logger, [{topic, Idx}]),
	iris_logger:info(Logger, "subscribing to new topic", [{name, Topic},
		{limits, lists:flatten(io_lib:format("1T|~pB", [MemoryLimit]))}
	]),

	% Execute the subscription procedure
	ok        = iris_proto:send_subscribe(Sock, Topic),
	{ok, Sub} = iris_topic:start_link(self(), Module, Args, MemoryLimit, Logger),
	Limiter   = iris_topic:limiter(Sub),
	true      = ets:insert_new(State#state.subLive, {Topic, Sub, Limiter}),
	{reply, ok, State#state{subIdx = Idx + 1}};

%% Relays an event to the Iris node for topic publishing.
handle_call({publish, Topic, Event}, _From, State = #state{sock = Sock}) ->
	iris_logger:debug(State#state.logger, "publishing new event", [{topic, Topic}, {data, Event}]),
	{reply, iris_proto:send_publish(Sock, Topic, Event), State};

%% Relays a subscription removal request to the Iris node (ensuring validity).
handle_call({unsubscribe, Topic}, _From, State = #state{sock = Sock}) ->
	% Look up the existing subscription and dump it from the list
	[{Topic, Sub, _Limiter}] = ets:lookup(State#state.subLive, Topic),
	true = ets:delete(State#state.subLive, Topic),

	% Terminate the subscription both locally and remotely
	ok = iris_topic:stop(Sub),
	{reply, iris_proto:send_unsubscribe(Sock, Topic), State};

%% Relays a tunneling request to the Iris node, waiting async with for the reply.
handle_call({tunnel, Cluster, Timeout}, From, State = #state{sock = Sock}) ->
	% Create a result channel for the tunneling reply
	TunId  = State#state.tunIdx,
	Logger = iris_logger:new(State#state.logger, [{tunnel, TunId}]),
	iris_logger:info(Logger, "constructing outbound tunnel",
		[{cluster, Cluster}, {timeout, Timeout}]
	),
	true = ets:insert_new(State#state.tunPend, {TunId, From, Logger}),

	{ok, Tunnel} = iris_tunnel:start_link(TunId, Logger),
	true = ets:insert_new(State#state.tunLive, {TunId, Tunnel}),

	% Send the request to the relay and finish with a pending reply
	ok = iris_proto:send_tunnel_init(Sock, TunId, Cluster, Timeout),
	{noreply, State#state{tunIdx = TunId+1}};

%% Relays a tunnel data packet to the Iris node.
handle_call({tunnel_send, TunId, SizeOrCont, Message}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:send_tunnel_transfer(Sock, TunId, SizeOrCont, Message), State};

%% Relays a tunnel data allowance to the Iris node.
handle_call({tunnel_allowance, TunId, Allowance}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:send_tunnel_allowance(Sock, TunId, Allowance), State};

%% Forwards a tunnel closing request to the relay if not yet closed remotely and
%% removes the tunnel from the local state.
handle_call({tunnel_close, TunId}, _From, State = #state{sock = Sock}) ->
	{reply, iris_proto:send_tunnel_close(Sock, TunId), State};

%% Retrieves the logger associated with the connection.
handle_call({logger}, _From, State = #state{logger = Logger}) ->
	{reply, Logger, State}.

%% Delivers a reply to a pending request.
handle_cast({handle_reply, Id, Response}, State) ->
	% Fetch the result channel and remove from state
	{Id, Pending} = hd(ets:lookup(State#state.reqPend, Id)),
	ets:delete(State#state.reqPend, Id),

	% Extract the response details and log it
	{Reply, Error} = case	Response of
		{ok, Rep}      -> {Rep, ""};
		{error, Fault} -> {<<>>, Fault}
	end,
	iris_logger:debug(State#state.logger, "request completed",
		[{local_request, Id}, {data, Reply}, {error, Error}]
	),

	% Reply to the pending process and return
	gen_server:reply(Pending, Response),
	{noreply, State};

%% Accepts an incoming tunneling request from a remote cluster, assembling a local
%% tunnel with the given chunking limit and replies to the relay with the final
%% permanent tunnel id.
handle_cast({handle_tunnel_init, BuildId, ChunkLimit}, State = #state{sock = Sock}) ->
	% Create the local tunnel endpoint
	TunId  = State#state.tunIdx,
	Logger = iris_logger:new(State#state.logger, [{tunnel, TunId}]),
	iris_logger:info(Logger, "accepting inbound tunnel", [{chunk_limit, ChunkLimit}]),

	{ok, Tunnel} = iris_tunnel:start_link(TunId, ChunkLimit, Logger),
	true = ets:insert_new(State#state.tunLive, {TunId, Tunnel}),

	% Acknowledge the tunnel creation to the relay
	ok = iris_proto:send_tunnel_confirm(Sock, BuildId, TunId),
	ok = iris_proto:send_tunnel_allowance(Sock, TunId, iris_limits:default_tunnel_buffer()),
	iris_logger:info(Logger, "tunnel acceptance completed"),

	% Notify the handler of the new tunnel
	ok = iris_server:handle_tunnel(State#state.handler, Tunnel),
	{noreply, State#state{tunIdx = TunId+1}};

% Delivers a reply to a pending tunneling request.
handle_cast({handle_tunnel_result, TunId, Result}, State = #state{sock = Sock}) ->
	% Fetch the result channel and remove from state
	{TunId, From, Logger} = hd(ets:lookup(State#state.tunPend, TunId)),
	{TunId, Tunnel}       = hd(ets:lookup(State#state.tunLive, TunId)),
	ets:delete(State#state.tunPend, TunId),

	% Finalize the tunnel construction
	ok = iris_tunnel:finalize(Tunnel, Result),

	% Reply to the pending process and return
	Reply = case Result of
		{ok, ChunkLimit} ->
			iris_logger:info(Logger, "tunnel construction completed", [{chunk_limit, ChunkLimit}]),
			ok = iris_proto:send_tunnel_allowance(Sock, TunId, iris_limits:default_tunnel_buffer()),
			{ok, Tunnel};
		{error, Reason} ->
			iris_logger:warn(Logger, "tunnel construction failed", [{reason, Reason}]),
			true = ets:delete(State#state.tunLive, TunId),
			{error, Reason}
	end,
	gen_server:reply(From, Reply),
	{noreply, State};

%% Closes a tunnel connection, removing it from the local state.
handle_cast({handle_tunnel_close, TunId}, State) ->
	true = ets:delete(State#state.tunLive, TunId),
	{noreply, State}.

%% Handles the termination of the receiver thread: either returns a clean exit
%% or notifies the handler of a drop.
handle_info({'EXIT', ProcPid, Reason}, State) when ProcPid =:= State#state.procpid ->
	% Notify all pending requests of the failure
	lists:foreach(fun({_ReqId, Pid}) ->
		gen_server:reply(Pid, {error, terminating})
	end, ets:tab2list(State#state.reqPend)),

	% Terminate, notifying either the closer or the handler
	case State#state.closer of
		nil ->
			State#state.handler ! {drop, Reason},
			{stop, Reason, State};
		Pid ->
			gen_server:reply(Pid, ok),
			{stop, normal, State}
	end;

%% Handles the normal termination of any child, beside the protocol processor.
handle_info({'EXIT', _Pid, normal}, State) ->
	{noreply, State}.


%% Final cleanup, close up the relay link.
terminate(_Reason, State) ->
	gen_tcp:close(State#state.sock).


%% =============================================================================
%% Unused generic server methods
%% =============================================================================

code_change(_OldVsn, _State, _Extra) ->
	{error, unimplemented}.
