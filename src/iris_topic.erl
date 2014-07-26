%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% @doc A behavior module for implementing an Iris topic subscription handler.
%%      The behavior follows similar design principles to the OTP gen_server.
%%
%%      It is assumed that all handler specific parts are located in a callback
%%      module, which implements and exports a set of pre-defined functions. The
%%      relationship between the Iris API and the callback functions is as follows:
%%
%%      ```
%%      iris_client module           callback module
%%      --------------               -------------------------
%%      iris_client:subscribe   ---> Module:init/2
%%      iris_client:publish     ---> Module:handle_event/2
%%      iris_client:unsubscribe ---> Module:terminate/2
%%      '''
%%
%%      If a callback function fails or returns a bad value, the iris_topic and
%%      parent {@link iris_client} or {@link iris_server} will terminate.
%%
%%      A slight difference to the gen_server behavior is that the event trigger
%%      methods have not been duplicated in the iris_topic module too, rather
%%      they use the methods defined in the {@link iris_client} module, as depicted
%%      in the above table.
%%
%%      == Callback methods ==
%%      Since edoc cannot generate documentation for callback methods, they have
%%      been included here for reference:
%%
%%      === init/2 ===
%%      ```
%%      init(Client, Args) -> {ok, State} | {stop, Reason}
%%          Client = iris_client:client()
%%          Args   = term()
%%          State  = term()
%%          Reason = term()
%%      '''
%%			Whenever an iris_topic is started using {@link iris_client:subscribe/4},
%%      this function is called in the new process to initialize the handler state.
%%
%%      <ul>
%%        <li>`Client' is the client or server's connection to the relay (depending
%%             who teh subscriber is, enabling queries to other remote services.</li>
%%        <li>`Args' is the `Args' argument provided to `subscribe'.</li>
%%      </ul><ul>
%%        <li>If the initialization succeeds, the function should return `{ok,
%%            State}'.</li>
%%        <li>Otherwise, the return value should be `{stop, Reason}'</li>
%%      </ul>
%%
%%      === handle_event/2 ===
%%      ```
%%      handle_event(Event, State) -> {noreply, NewState} | {stop, Reason, NewState}
%%          Event    = binary()
%%          State    = term()
%%          NewState = term()
%%          Reason   = term()
%%      '''
%%      Whenever an iris_topic receives an event sent using {@link iris_client:publish/3},
%%      this function is called to handle it.
%%
%%      <ul>
%%        <li>`Event' is the `Event' argument provided to `publish'.</li>
%%        <li>`State' is the internal state of the iris_topic.</li>
%%      </ul><ul>
%%        <li>If the function returns `{noreply, NewState}', the iris_topic
%%            will continue executing with `NewState'.</li>
%%        <li>If the return value is `{stop, Reason, NewState}', the iris_topic
%%            will call `Module:terminate/2' and terminate.</li>
%%      </ul>
%%
%%      === terminate/2 ===
%%      ```
%%      terminate(Reason, State)
%%          Reason = term()
%%          State  = term()
%%      '''
%%      This method is called when an iris_topic is about to terminate. It is
%%      the opposite of `Module:init/1' and should do any necessary cleaning up.
%%      The return value is ignored, after which the handler process stops.
%%
%%      <ul>
%%        <li>`Reason' is the term denoting the stop reason. If the iris_topic
%%            is terminating due to a method returning `{stop, ...}', `Reason'
%%            will have the value specified in that tuple.</li>
%%        <li>`State' is the internal state of the iris_topic.</li>
%%      </ul>
%% @end

-module(iris_topic).
-export([start_link/5, stop/1, limiter/1]).
-export([handle_event/2]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
	code_change/3]).


%% =============================================================================
%% Iris topic subscription behavior definitions
%% =============================================================================

-callback init(Client :: iris_client:client(), Args :: term()) ->
	{ok, State :: term()} | {stop, Reason :: term()}.

-callback handle_event(Event :: binary(), State :: term()) ->
	{noreply, NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback terminate(Reason :: term(), State :: term()) ->
	no_return().


%% =============================================================================
%% Internal API functions
%% =============================================================================

%% @private
%% Starts and links a topic subscription.
-spec start_link(Conn :: pid(), Module :: atom(), Args :: term(),	Limits :: pos_integer(),
  Logger :: iris_logger:logger()) -> {ok, Server :: pid()} | {error, Reason :: term()}.

start_link(Conn, Module, Args, Limits, Logger) ->
	gen_server:start_link(?MODULE, {Conn, Module, Args, Limits, Logger}, []).


%% @private
%% Terminates a topic subscription.
-spec stop(Topic :: pid()) ->
	ok | {error, Reason :: term()}.

stop(Topic) ->
	gen_server:call(Topic, stop).

%% @private
%% Fetches the mailbox limiter of the topic.
-spec limiter(Topic :: pid()) -> pid().

limiter(Topic) ->
	gen_server:call(Topic, limiter).


%% @private
%% Schedules a published event for the subscription handler to process.
-spec handle_event(Limiter :: pid(), Event :: binary()) -> ok.

handle_event(Limiter, Event) ->
  ok = iris_mailbox:schedule(Limiter, {handle_event, Event}).


%% =============================================================================
%% Generic server internal state
%% =============================================================================

-record(state, {
	limiter,    %% Bounded mailbox limiter
  hand_mod,   %% Handler callback module
  hand_state, %% Handler internal state
  logger      %% Logger with connection and topic id injected
}).


%% =============================================================================
%% Generic server callback methods
%% =============================================================================

%% @private
%% Initializes the callback handler and subscribes to the requested topic.
init({Conn, Module, Args, Limits, Logger}) ->
	% Spawn the mailbox limiter threads
	process_flag(trap_exit, true),
  Limiter = iris_mailbox:start_link(self(), Limits, Logger),

  % Initialize the callback handler
	case Module:init(Conn, Args) of
		{ok, State} ->
			{ok, #state{
				limiter    = Limiter,
				hand_mod   = Module,
				hand_state = State,
        logger     = Logger
			}};
		{error, Reason} -> {stop, Reason}
	end.


%% @private
%% Retrieves the bounded mailbox limiter.
handle_call(limiter, _From, State = #state{limiter = Limiter}) ->
	{reply, Limiter, State};

%% Unsubscribes from the topic.
handle_call(stop, _From, State) ->
  iris_logger:info(State#state.logger, "unsubscribing from topic"),
	{stop, normal, ok, State}.


%% @private
%% Delivers a topic event to the callback and processes the result.
handle_info({LogCtx, {handle_event, Event}, Limiter}, State = #state{hand_mod = Mod}) ->
  iris_logger:debug(State#state.logger, "handling scheduled event", [LogCtx]),
  iris_mailbox:replenish(Limiter, byte_size(Event)),
	case Mod:handle_event(Event, State#state.hand_state) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end.


%% @private
%% Notifies the callback of the termination.
terminate(Reason, State = #state{hand_mod = Mod}) ->
	Mod:terminate(Reason, State#state.hand_state).


%% =============================================================================
%% Unused generic server methods
%% =============================================================================

%% @private
code_change(_OldVsn, _State, _Extra) ->
	{error, unimplemented}.

%% @private
handle_cast(_Request, State) ->
	{stop, unimplemented, State}.
