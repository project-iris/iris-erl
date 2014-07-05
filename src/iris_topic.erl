%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(iris_topic).
-export([start_link/4, stop/1, limiter/1]).
-export([handle_event/2]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
	code_change/3]).

-define(DEFAULT_EVENT_MEMORY_LIMIT, 64 * 1024 * 1024).


%% =============================================================================
%% Iris topic subscription behavior definitions
%% =============================================================================

-callback init(Conn :: pid(), Args :: term()) ->
	{ok, State :: term()} | {stop, Reason :: term()}.

-callback handle_event(Event :: binary(), State :: term()) ->
	{noreply, NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback terminate(Reason :: term(), State :: term()) ->
	no_return().


%% =============================================================================
%% External API functions
%% =============================================================================

-spec start_link(Conn :: pid(), Module :: atom(), Args :: term(),
	Options :: [{atom(), term()}]) -> {ok, Server :: pid()} | {error, Reason :: term()}.

start_link(Conn, Module, Args, Options) ->
	gen_server:start_link(?MODULE, {Conn, Module, Args, Options}, []).


-spec stop(Topic :: pid()) ->
	ok | {error, Reason :: term()}.

stop(Topic) ->
	gen_server:call(Topic, stop).

-spec limiter(Topic :: pid()) -> pid().

limiter(Topic) ->
	gen_server:call(Topic, limiter).

%% =============================================================================
%% Internal API callback functions
%% =============================================================================

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
  hand_state  %% Handler internal state
}).


%% =============================================================================
%% Generic server callback methods
%% =============================================================================

%% @private
%% Initializes the callback handler and subscribes to the requested topic.
init({Conn, Module, Args, Options}) ->
  % Load the service limits (or defaults)
  EventMemory = case proplists:lookup(event_memory, Options) of
    none                  -> ?DEFAULT_EVENT_MEMORY_LIMIT;
    {event_memory, Limit} -> Limit
  end,

	% Spawn the mailbox limiter threads
	process_flag(trap_exit, true),
  Limiter = iris_mailbox:start_link(self(), EventMemory, iris_logger:new()),

  % Initialize the callback handler
	case Module:init(Conn, Args) of
		{ok, State} ->
			{ok, #state{
				limiter    = Limiter,
				hand_mod   = Module,
				hand_state = State
			}};
		{error, Reason} -> {stop, Reason}
	end.


%% @private
%% Retrieves the bounded mailbox limiter.
handle_call(limiter, _From, State = #state{limiter = Limiter}) ->
	{reply, Limiter, State};

%% Unsubscribes from the topic.
handle_call(stop, _From, State) ->
	{stop, normal, shutdown, State}.


%% @private
%% Delivers a topic event to the callback and processes the result.
handle_info({LogCtx, {handle_event, Event}, Limiter}, State = #state{hand_mod = Mod}) ->
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
