%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(iris_server).
-export([start/4, start/5, start_link/4, start_link/5, stop/1, reply/2, logger/1]).
-export([handle_broadcast/2, handle_request/4, handle_tunnel/2]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
	code_change/3]).


%% =============================================================================
%% Iris server behavior definitions
%% =============================================================================

-callback init(Conn :: pid(), Args :: term()) ->
	{ok, State :: term()} | {stop, Reason :: term()}.

-callback handle_broadcast(Message :: binary(), State :: term()) ->
	{noreply, NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback handle_request(Request :: binary(), From :: iris:sender(), State :: term()) ->
	{reply, {ok, Reply :: binary()} | {error, Reason :: term()}, NewState :: term()} | {noreply, NewState :: term()} |
	{stop, Reply :: binary(), Reason :: term(), NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback handle_tunnel(Tunnel :: iris:tunnel(), State :: term()) ->
	{noreply, NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback handle_drop(Reason :: term(), State :: term()) ->
	{noreply, NewState :: term()} | {stop, NewReason :: term(), NewState :: term()}.

-callback terminate(Reason :: term(), State :: term()) ->
	no_return().


%% =============================================================================
%% External API functions
%% =============================================================================

%% No option version of the extended start method. See the next method below.
-spec start(Port :: pos_integer(), Cluster :: string(), Module :: atom(), Args :: term()) ->
	{ok, Server :: pid()} | {error, Reason :: term()}.

start(Port, Cluster, Module, Args) ->
	start(Port, Cluster, Module, Args, []).

%% @doc Connects to the Iris network and registers a new service instance as a
%%      member of the specified service cluster.
%%
%%      All the inbound network events will be received by the specified handler
%%      module, conforming to the iris_server behavior.
%%
%% @spec (Port, Cluster, Module, Args, Options) -> {ok, Server} | {error, Reason}
%%      Port    = pos_integer()
%%      Cluster = string()
%%      Module  = atom()
%%      Args    = term()
%%      Options = [Option]
%%        Option = {broadcast_memory, Limit} | {request_memory, Limit}
%%          Limit = pos_integer()
%%      Server  = pid()
%%      Reason  = term()
%% @end
-spec start(Port :: pos_integer(), Cluster :: string(), Module :: atom(),
	Args :: term(), Options :: [{atom(), term()}]) ->
	{ok, Server :: pid()} | {error, Reason :: term()}.

start(Port, Cluster, Module, Args, Options) ->
	gen_server:start(?MODULE, {Port, Cluster, Module, Args, Options}, []).


%% No option version of the extended start_link method. See the next method below.
-spec start_link(Port :: pos_integer(), Cluster :: string(), Module :: atom(), Args :: term()) ->
	{ok, Server :: pid()} | {error, Reason :: term()}.

start_link(Port, Cluster, Module, Args) ->
	start_link(Port, Cluster, Module, Args, []).

%% @doc Connects to the Iris network and registers a new service instance as a
%%      member of the specified service cluster, linking it to the caller.
%%
%%      All the inbound network events will be received by the specified handler
%%      module, conforming to the iris_server behavior.
%%
%% @spec (Port, Cluster, Module, Args, Options) -> {ok, Server} | {error, Reason}
%%      Port    = pos_integer()
%%      Cluster = string()
%%      Module  = atom()
%%      Args    = term()
%%      Options = [Option]
%%        Option = {broadcast_memory, Limit} | {request_memory, Limit}
%%          Limit = pos_integer()
%%      Server  = pid()
%%      Reason  = term()
%% @end
-spec start_link(Port :: pos_integer(), Cluster :: string(), Module :: atom(),
	Args :: term(), Options :: [{atom(), term()}]) ->
	{ok, Server :: pid()} | {error, Reason :: term()}.

start_link(Port, Cluster, Module, Args, Options) ->
	gen_server:start_link(?MODULE, {Port, Cluster, Module, Args, Options}, []).


%% @doc Unregisters the service instance from the Iris network, removing all
%%      subscriptions and closing all active tunnels.
%%
%%      The call blocks until the tear-down is confirmed by the Iris node.
%%
%% @spec (Server) -> ok | {error, Reason}
%%      Server     = pid()
%%      Reason     = atom()
%% @end
-spec stop(Server :: pid()) ->
	ok | {error, Reason :: term()}.

stop(Server) ->
	gen_server:call(Server, stop).


%% @doc This function can be used by an iris_server to explicitly send a reply
%%      to a client that called request/4 when the reply cannot be defined in
%%      the return value of Module:handle_request/3.
%%
%%      Client must be the From argument provided to the callback function.
%%
%% @spec (Client, Reply) -> ok | {error, Reason}
%%      Client = sender()
%%      Reply  = binary()
%%      Reason = atom()
%% @end
-spec reply(Client :: term(), Reply :: binary()) ->
	ok | {error, Reason :: atom()}.

reply(Client, Reply) ->
	iris_conn:reply(Client, Reply).


logger(Server) ->
	gen_server:call(Server, {logger}, infinity).


%% =============================================================================
%% Internal API callback functions
%% =============================================================================

%% @private
%% Schedules an application broadcast for the service handler to process.
-spec handle_broadcast(Limiter :: pid(), Message :: binary()) -> ok.

handle_broadcast(Limiter, Message) ->
	ok = iris_mailbox:schedule(Limiter, {handle_broadcast, Message}).


%% @private
%% Schedules an application request for the service handler to process.
-spec handle_request(Limiter :: pid(), Id :: non_neg_integer(), Request :: binary(),
	Timeout :: pos_integer()) -> ok.

handle_request(Limiter, Id, Request, Timeout) ->
	Expiry = timestamp() + Timeout * 1000,
	ok = iris_mailbox:schedule(Limiter, {handle_request, Id, Request, Timeout, Expiry}).


%% @private
%%
-spec handle_tunnel(Server :: pid(), Tunnel :: pid()) -> ok.

handle_tunnel(Server, Tunnel) ->
	ok = gen_server:cast(Server, {handle_tunnel, Tunnel}).


%% =============================================================================
%% Internal helper functions
%% =============================================================================

%% Retrieves the current monotonic (erlang:now) timer's timestamp.
-spec timestamp() -> pos_integer().

timestamp() ->
	{Mega, Secs, Micro} = erlang:now(),
	Mega*1000*1000*1000*1000 + Secs * 1000 * 1000 + Micro.


%% =============================================================================
%% Generic server internal state
%% =============================================================================

-record(state, {
	conn,       %% High level Iris connection
	hand_mod,   %% Handler callback module
	hand_state, %% Handler internal state
	logger      %% Logger with connection id injected
}).


%% =============================================================================
%% Generic server callback methods
%% =============================================================================

%% @private
%% Initializes the callback handler and connects to the local Iris relay node.
init({Port, Cluster, Module, Args, Options}) ->
	%% Make sure the service limits have valid values
	BroadcastMemory = case proplists:lookup(broadcast_memory, Options) of
		none                       -> iris_limits:default_broadcast_memory();
		{broadcast_memory, BLimit} -> BLimit
	end,
	RequestMemory = case proplists:lookup(request_memory, Options) of
		none                     -> iris_limits:default_request_memory();
		{request_memory, RLimit} -> RLimit
	end,
	Limits = {BroadcastMemory, RequestMemory},

	Logger = iris_logger:new([{server, iris_counter:next_id(server)}]),
	iris_logger:info(Logger, "registering new service", [
		{relay_port, Port}, {cluster, Cluster},
		{broadcast_limits, lists:flatten(io_lib:format("1T|~pB", [element(1, Limits)]))},
		{request_limits, lists:flatten(io_lib:format("1T|~pB", [element(2, Limits)]))}
	]),

	% Initialize the Iris connection
	case iris_conn:register(Port, lists:flatten(Cluster), self(), Limits, Logger) of
		{ok, Conn} ->
			% Initialize the callback handler
			case Module:init(Conn, Args) of
				{ok, State} ->
					iris_logger:info(Logger, "service registration completed"),
					{ok, #state{
						conn       = Conn,
						hand_mod   = Module,
						hand_state = State,
						logger     = Logger
					}};
				{error, Reason} ->
					iris_logger:info(Logger, "user failed to initialize service", [{reason, Reason}]),
					{stop, Reason}
			end;
		{error, Reason} ->
			iris_logger:info(Logger, "failed to register new service", [{reason, Reason}]),
			{stop, Reason}
	end.

%% @private
%% Closes the Iris connection, returning the result to the caller.
handle_call(stop, _From, State = #state{conn = Conn}) ->
	{stop, normal, iris_conn:close(Conn), State#state{conn = nil}};

%% Retrieves the logger associated with the server.
handle_call({logger}, _From, State = #state{logger = Logger}) ->
	{reply, Logger, State}.

%% @private
%% Delivers a broadcast message to the callback and processes the result.
handle_info({LogCtx, {handle_broadcast, Message}, Limiter}, State = #state{hand_mod = Mod}) ->
	iris_logger:debug(State#state.logger, "handling scheduled broadcast", [LogCtx]),
	iris_mailbox:replenish(Limiter, byte_size(Message)),
	case Mod:handle_broadcast(Message, State#state.hand_state) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end;

%% Delivers a request to the callback and processes the result.
handle_info({LogCtx, {handle_request, Id, Request, Timeout, Expiry}, Limiter}, State = #state{conn = Conn, hand_mod = Mod}) ->
	iris_mailbox:replenish(Limiter, byte_size(Request)),

	Now = timestamp(),
	case Expiry < Now of
		true  ->
			iris_logger:error(State#state.logger, "dumping expired scheduled request",
				[LogCtx, {scheduled, Timeout + (Now - Expiry) / 1000}, {timeout, Timeout}, {expired, (Now - Expiry) / 1000}]
			),
			{noreply, State};
		false ->
			iris_logger:debug(State#state.logger, "handling scheduled request", [LogCtx]),
			From = {Conn, Id},
			case Mod:handle_request(Request, From, State#state.hand_state) of
				{reply, Response, NewState} ->
					ok = iris_conn:reply(From, Response),
					{noreply, State#state{hand_state = NewState}};
				{noreply, NewState} ->
					{noreply, State#state{hand_state = NewState}};
				{stop, Reason, Reply, NewState} ->
					ok = iris_conn:reply(From, Reply),
					{stop, Reason, State#state{hand_state = NewState}};
				{stop, Reason, NewState} ->
					{stop, Reason, State#state{hand_state = NewState}}
			end
	end;

%% Notifies the callback of the connection drop and processes the result.
handle_info({drop, Reason}, State = #state{conn = Conn, hand_mod = Mod}) ->
	case Mod:handle_drop(Reason, State#state.hand_state, Conn) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end.

%% @private
%% Delivers an inbound tunnel to the callback and processes the result.
handle_cast({handle_tunnel, Tunnel}, State = #state{conn = Conn, hand_mod = Mod}) ->
	case Mod:handle_tunnel(Tunnel, State#state.hand_state) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end.

%% @private
%% Notifies the callback of the termination and closes the link if still up.
terminate(Reason, State = #state{conn = Conn, hand_mod = Mod}) ->
	Mod:terminate(Reason, State#state.hand_state),
	case Conn of
		nil -> ok;
		_   -> iris_conn:close(Conn)
	end.

%% =============================================================================
%% Unused generic server methods
%% =============================================================================

%% @private
code_change(_OldVsn, _State, _Extra) ->
	{error, unimplemented}.
