%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% @doc A behavior module for implementing an Iris server event handler. The
%%      behavior follows the exact same design principles as the OTP gen_server.
%%
%%      It is assumed that all handler specific parts are located in a callback
%%      module, which implements and exports a set of pre-defined functions. The
%%      relationship between the Iris API and the callback functions is as
%%      follows:
%%
%%      ```
%%      iris_server module          callback module
%%      ----------------------      --------------------
%%      iris_server:start
%%      iris_server:start_link ---> Module:init/1
%%      iris_server:stop       ---> Module:terminate/2
%%      ---                    ---> Module:handle_drop/2
%%
%%      iris module                 callback module
%%      --------------              -------------------------
%%      iris:broadcast         ---> Module:handle_broadcast/3
%%      iris:request           ---> Module:handle_request/4
%%      iris:publish           ---> Module:handle_publish/4
%%      iris:tunnel            ---> Module:handle_tunnel/3
%%      '''
%%
%%      If a callback function fails or returns a bad value, the iris_server
%%      will terminate. Unless otherwise stated, all functions in this module
%%      fail if the specified gen_server does not exist or if bad arguments are
%%      given.
%%
%%      A slight difference to the gen_server behavior is that the event trigger
%%      methods have not been duplicated in the iris_server module too, rather
%%      they use the methods defined in the {@link iris} module, as depicted in
%%      the above table.
%%
%%      == Callback methods ==
%%      Since edoc cannot generate documentation for callback methods, they have
%%      been included here for reference:
%%
%%      === init/1 ===
%%      ```
%%      init(Args) -> {ok, State} | {stop, Reason}
%%          Args   = term()
%%          State  = term()
%%          Reason = term()
%%      '''
%%			Whenever an iris_server is started using {@link iris_server:start/4} or
%%      {@link iris_server:start_link/4}, this function is called in the new
%%      process to initialize the handler state.
%%
%%      <ul>
%%        <li>`Args' is the `Args' argument provided to `start/start_link'.</li>
%%      </ul><ul>
%%        <li>If the initialization succeeds, the function should return `{ok,
%%            State}'.</li>
%%        <li>Otherwise, the return value should be `{stop, Reason}'</li>
%%      </ul>
%%
%%      === handle_broadcast/3 ===
%%      ```
%%      handle_broadcast(Message, State, Conn) -> {noreply, NewState} | {stop, Reason, NewState}
%%          Message  = binary()
%%          State    = term()
%%          Conn     = iris:connection()
%%          NewState = term()
%%          Reason   = term()
%%      '''
%%      Whenever an iris_server receives a message sent using {@link iris:broadcast/3},
%%      this function is called to handle it.
%%
%%      <ul>
%%        <li>`Message' is the `Message' argument provided to `broadcast'.</li>
%%        <li>`State' is the internal state of the iris_server.</li>
%%        <li>`Conn' is the relay connection to the Iris network if the handler
%%            needs additional communication.</li>
%%      </ul><ul>
%%        <li>If the function returns `{noreply, NewState}', the iris_server
%%            will continue executing with `NewState'.</li>
%%        <li>If the return value is `{stop, Reason, NewState}', the iris_server
%%            will call `Module:terminate/2' and terminate.</li>
%%      </ul>
%%
%%      === handle_request/4 ===
%%      ```
%%      handle_request(Request, From, State, Conn) ->
%%              {reply, Reply, NewState} | {noreply, NewState} |
%%              {stop, Reason, Reply, NewState} | {stop, Reason, NewState} |
%%          Request  = binary()
%%          From     = iris:sender()
%%          State    = term()
%%          Conn     = iris:connection()
%%          Reply    = binary()
%%          NewState = term()
%%          Reason   = term()
%%      '''
%%      Whenever an iris_server receives a request sent using {@link iris:request/4},
%%      this function is called to handle it.
%%
%%      <ul>
%%        <li>`Request' is the `Request' argument provided to `request'.</li>
%%        <li>`From' is the sender address used by {@link iris:reply/2} to send
%%            an asynchronous reply back.</li>
%%        <li>`State' is the internal state of the iris_server.</li>
%%        <li>`Conn' is the relay connection to the Iris network if the handler
%%            needs additional communication.</li>
%%      </ul><ul>
%%        <li>If the function returns `{reply, Reply, NewState}', the `Reply'
%%            will be given back to `From' as the return value to `request'. The
%%            iris_server then will continue executing using `NewState'.</li>
%%        <li>If the function returns `{noreply, NewState}', the iris_server
%%            will continue executing with `NewState'. Any reply to `From' must
%%            be sent back explicitly using {@link iris:reply/2}.</li>
%%        <li>If the return value is `{stop, Reason, Reply, NewState}', `Reply'
%%            will be sent back to `From', and if `{stop, Reason, NewState}' is
%%            used, any reply must be sent back explicitly. In either case the
%%            iris_server will call `Module:terminate/2' and terminate.</li>
%%      </ul>
%%
%%      === handle_publish/4 ===
%%      ```
%%      handle_publish(Topic, Event, State, Conn) -> {noreply, NewState} | {stop, Reason, NewState}
%%          Topic    = [byte()]
%%          Event    = binary()
%%          State    = term()
%%          Conn     = iris:connection()
%%          NewState = term()
%%          Reason   = term()
%%      '''
%%      Whenever an iris_server receives an event sent using {@link iris:publish/3},
%%      on a subscribed topic, this function is called to handle it.
%%
%%      <ul>
%%        <li>`Topic' is the `Topic' argument provided to `publish'.</li>
%%        <li>`Event' is the `Event' argument provided to `publish'.</li>
%%        <li>`State' is the internal state of the iris_server.</li>
%%        <li>`Conn' is the relay connection to the Iris network if the handler
%%            needs additional communication.</li>
%%      </ul><ul>
%%        <li>If the function returns `{noreply, NewState}', the iris_server
%%            will continue executing with `NewState'.</li>
%%        <li>If the return value is `{stop, Reason, NewState}', the iris_server
%%            will call `Module:terminate/2' and terminate.</li>
%%      </ul>
%%
%%      === handle_tunnel/3 ===
%%      ```
%%      handle_tunnel(Tunnel, State, Conn) -> {noreply, NewState} | {stop, Reason, NewState}
%%          Topic    = iris:tunnel()
%%          State    = term()
%%          Conn     = iris:connection()
%%          NewState = term()
%%          Reason   = term()
%%      '''
%%      Whenever an iris_server receives an inbound tunnel connection sent using
%%      {@link iris:tunnel/3}, this function is called to handle it.
%%
%%      <ul>
%%        <li>`Tunnel' is the Iris data stream used for ordered messaging.</li>
%%        <li>`State' is the internal state of the iris_server.</li>
%%        <li>`Conn' is the relay connection to the Iris network if the handler
%%            needs additional communication.</li>
%%      </ul><ul>
%%        <li>If the function returns `{noreply, NewState}', the iris_server
%%            will continue executing with `NewState'.</li>
%%        <li>If the return value is `{stop, Reason, NewState}', the iris_server
%%            will call `Module:terminate/2' and terminate.</li>
%%      </ul>
%%
%%      Note, handling tunnel connections should be done in spawned children
%%      processes in order not to block the handler process. The `handle_tunnel'
%%      method itself is called synchronously in order to allow modifications to
%%      the internal state if need be.
%%
%%      === handle_drop/2 ===
%%      ```
%%      handle_drop(Reason, State) -> {noreply, NewState} | {stop, NewReason, NewState}
%%          Reason    = term()
%%          State     = term()
%%          NewState  = term()
%%          NewReason = term()
%%      '''
%%      Error handler called in the event of an unexpected remote closure of the
%%      relay connection to the local Iris node.
%%
%%      <ul>
%%        <li>`Reason' is the encountered problem (most probably an EOF).</li>
%%        <li>`State' is the internal state of the iris_server.</li>
%%      </ul><ul>
%%        <li>If the function returns `{noreply, NewState}', the iris_server
%%            will continue executing with `NewState', though no new data will
%%            arrive, nor can be sent.</li>
%%        <li>If the return value is `{stop, NewReason, NewState}', the process
%%            will call `Module:terminate/2' and terminate.</li>
%%      </ul>
%%
%%      === terminate/2 ===
%%      ```
%%      terminate(Reason, State)
%%          Reason    = term()
%%          State     = term()
%%      '''
%%      This method is called when an iris_server is about to terminate. It is
%%      the opposite of `Module:init/1' and should do any necessary cleaning up.
%%      The return value is ignored, after which the handler process stops.
%%
%%      <ul>
%%        <li>`Reason' is the term denoting the stop reason. If the iris_server
%%            is terminating due to a method returning `{stop, ...}', `Reason'
%%            will have the value specified in that tuple.</li>
%%        <li>`State' is the internal state of the iris_server.</li>
%%      </ul>
%% @end

-module(iris_server).
-export([start/4, start/5, start_link/4, start_link/5, stop/1]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
	code_change/3]).

%% =============================================================================
%% Iris server behavior definitions
%% =============================================================================

-callback init(Args :: term()) ->
	{ok, State :: term()} | {stop, Reason :: term()}.

-callback handle_broadcast(Message :: binary(), State :: term(), Conn :: iris:connection()) ->
	{noreply, NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback handle_request(Request :: binary(), From :: iris:sender(), State :: term(), Conn :: iris:connection()) ->
	{reply, Reply :: binary(), NewState :: term()} | {noreply, NewState :: term()} |
	{stop, Reply :: binary(), Reason :: term(), NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback handle_publish(Topic :: string(), Event :: binary(), State :: term(), Conn :: iris:connection()) ->
	{noreply, NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback handle_tunnel(Tunnel :: iris:tunnel(), State :: term(), Conn :: iris:connection()) ->
	{noreply, NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback handle_drop(Reason :: term(), State :: term()) ->
	{noreply, NewState :: term()} | {stop, NewReason :: term(), NewState :: term()}.

-callback terminate(Reason :: term(), State :: term()) ->
	no_return().


%% =============================================================================
%% External API functions
%% =============================================================================

-spec start(Port :: pos_integer(), Cluster :: string(), Module :: atom(), Args :: term()) ->
	{ok, Server :: pid()} | {error, Reason :: term()}.

start(Port, Cluster, Module, Args) ->
	start(Port, Cluster, Module, Args, []).


%% @doc Creates and starts an Iris server process.
%%
%%      The server will connect to the locally running Iris relay, and receive
%%      all the inbound network events, conforming to the iris_server behavior.
%%
%%      <ul>
%%        <li>`Port' is the local TCP endpoint on which the Iris relay is
%%             listening.</li>
%%        <li>`Cluster' is the application group the server wishes to join.</li>
%%        <li>`Module' is the name of the callback module</li>
%%        <li>`Args' is an arbitrary term provided to `Module:init/1'</li>
%%      </ul><ul>
%%        <li>If the iris_server is successfully initialized, started and the
%%            relay node connection made, `{ok, Server, Connection}' is returned,
%%            where `Server' is the pid of the iris_server and `Connection' is
%%            the data link to the relay to allow communication from outside the
%%            iris_server handler too.</li>
%%        <li>If `Module:init/1' fails or the connection to a local Iris node
%%            cannot be made, the method returns with `{error, Reason}'.</li>
%%      </ul>
%%
%% @spec (Port, Cluster, Module, Args, Options) -> {ok, Server, Connection} | {error, Reason}
%%      Port       = pos_integer()
%%      Cluster        = string()
%%      Module     = atom()
%%      Args       = term()
%%      Server     = pid()
%%      Connection = connection()
%%      Reason     = atom()
%% @end
-spec start(Port :: pos_integer(), Cluster :: string(), Module :: atom(),
	Args :: term(), Options :: [{atom(), term()}]) ->
	{ok, Server :: pid()} | {error, Reason :: term()}.

start(Port, Cluster, Module, Args, Options) ->
	gen_server:start(?MODULE, {Port, Cluster, Module, Args}, []).


-spec start_link(Port :: pos_integer(), Cluster :: string(), Module :: atom(), Args :: term()) ->
	{ok, Server :: pid()} | {error, Reason :: term()}.

start_link(Port, Cluster, Module, Args) ->
	start_link(Port, Cluster, Module, Args, []).


%% @doc Creates and starts an Iris server process, linked to the caller.
%%
%%      The server will connect to the locally running Iris relay, and receive
%%      all the inbound network events, conforming to the iris_server behavior.
%%
%%      <ul>
%%        <li>`Port' is the local TCP endpoint on which the Iris relay is
%%             listening.</li>
%%        <li>`Cluster' is the application group the server wishes to join.</li>
%%        <li>`Module' is the name of the callback module</li>
%%        <li>`Args' is an arbitrary term provided to `Module:init/1'</li>
%%      </ul><ul>
%%        <li>If the iris_server is successfully initialized, started and the
%%            relay node connection made, `{ok, Server, Connection}' is returned,
%%            where `Server' is the pid of the iris_server and `Connection' is
%%            the data link to the relay to allow communication from outside the
%%            iris_server handler too.</li>
%%        <li>If `Module:init/1' fails or the connection to a local Iris node
%%            cannot be made, the method returns with `{error, Reason}'.</li>
%%      </ul>
%%
%% @spec (Port, Cluster, Module, Args, Options) -> {ok, Server, Connection} | {error, Reason}
%%      Port       = pos_integer()
%%      Cluster        = string()
%%      Module     = atom()
%%      Args       = term()
%%      Server     = pid()
%%      Connection = connection()
%%      Reason     = atom()
%% @end
-spec start_link(Port :: pos_integer(), Cluster :: string(), Module :: atom(),
	Args :: term(), Options :: [{atom(), term()}]) ->
	{ok, Server :: pid()} | {error, Reason :: term()}.

start_link(Port, Cluster, Module, Args, Options) ->
	gen_server:start_link(?MODULE, {Port, Cluster, Module, Args}, []).


%% @doc Gracefully terminates the server process.
%%
%%      <ul>
%%        <li>`Server' is the pid of the iris_server to terminate.</li>
%%      </ul>
%%
%% @spec (Server) -> ok | {error, Reason}
%%      Server     = pid()
%%      Reason     = atom()
%% @end
-spec stop(Server :: pid()) ->
	ok | {error, Reason :: term()}.

stop(Server) ->
	gen_server:call(Server, stop).


%% =============================================================================
%% Generic server internal state
%% =============================================================================

-record(state, {
	conn,       %% High level Iris connection
  hand_mod,   %% Handler callback module
  hand_state  %% Handler internal state
}).


%% =============================================================================
%% Generic server callback methods
%% =============================================================================

%% @private
%% Initializes the callback handler and connects to the local Iris relay node.
init({Port, Cluster, Module, Args}) ->
  % Initialize the callback handler
	case Module:init(Args) of
		{ok, State} ->
			% Initialize the Iris connection
			case iris_conn:register(Port, lists:flatten(Cluster), self()) of
				{ok, Conn} ->
					{ok, #state{
						conn       = Conn,
						hand_mod   = Module,
						hand_state = State
					}};
				{error, Reason} -> {stop, Reason}
			end;
		Error -> Error
	end.

%% @private
%% Closes the Iris connection, returning the result to the caller.
handle_call(stop, _From, State = #state{conn = Conn}) ->
	{stop, normal, iris_conn:close(Conn), State#state{conn = nil}}.

%% @private
%% Delivers a broadcast message to the callback and processes the result.
handle_info({broadcast, Message}, State = #state{conn = Conn, hand_mod = Mod}) ->
	case Mod:handle_broadcast(Message, State#state.hand_state, Conn) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end;

%% Delivers a request to the callback and processes the result.
handle_info({request, From, Request}, State = #state{conn = Conn, hand_mod = Mod}) ->
	case Mod:handle_request(Request, From, State#state.hand_state, Conn) of
		{reply, Reply, NewState} ->
			ok = iris:reply(From, Reply),
			{noreply, State#state{hand_state = NewState}};
		{noreply, NewState} ->
			{noreply, State#state{hand_state = NewState}};
		{stop, Reason, Reply, NewState} ->
			ok = iris:reply(From, Reply),
			{stop, Reason, State#state{hand_state = NewState}};
		{stop, Reason, NewState} ->
			{stop, Reason, State#state{hand_state = NewState}}
	end;

%% Delivers a publish event to the callback and processes the result.
handle_info({publish, Topic, Event}, State = #state{conn = Conn, hand_mod = Mod}) ->
	case Mod:handle_publish(Topic, Event, State#state.hand_state, Conn) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end;

%% Delivers an inbound tunnel to the callback and processes the result.
handle_info({tunnel, Tunnel}, State = #state{conn = Conn, hand_mod = Mod}) ->
	case Mod:handle_tunnel(Tunnel, State#state.hand_state, Conn) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end;

%% Notifies the callback of the connection drop and processes the result.
handle_info({drop, Reason}, State = #state{conn = Conn, hand_mod = Mod}) ->
	case Mod:handle_drop(Reason, State#state.hand_state, Conn) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end.

%% @private
%% Notifies the callback of the termination and closes the link if still up.
terminate(Reason, State = #state{conn = Conn, hand_mod = Mod}) ->
	Mod:terminate(Reason, State#state.hand_state),
	case Conn of
		nil -> ok;
		_   -> iris:close(Conn)
	end.

%% =============================================================================
%% Unused generic server methods
%% =============================================================================

%% @private
code_change(_OldVsn, _State, _Extra) ->
	{error, unimplemented}.

%% @private
handle_cast(_Request, State) ->
	{stop, unimplemented, State}.
