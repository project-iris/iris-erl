%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

-module(iris_server).
-export([start/4, start_link/4, broadcast/3, request/4, reply/2, subscribe/2,
	publish/3, unsubscribe/2, tunnel/3, close/1]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
	code_change/3]).

%% =============================================================================
%% Iris server behaviour definitions
%% =============================================================================

-export([behaviour_info/1]).

behaviour_info(callbacks) -> [
	{init, 1},
	{handle_broadcast, 2},
	{handle_request, 3},
	{handle_publish, 3},
	{handle_tunnel, 2},
	{handle_drop, 2},
	{terminate, 2}
].


%% =============================================================================
%% External API functions
%% =============================================================================

start(Port, App, Module, Args) ->
	gen_server:start(?MODULE, {Port, App, Module, Args}, []).

start_link(Port, App, Module, Args) ->
	gen_server:start_link(?MODULE, {Port, App, Module, Args}, []).

broadcast(ServerRef, App, Message) ->
	gen_server:call(ServerRef, {broadcast, App, Message}, infinity).

request(ServerRef, App, Request, Timeout) ->
	gen_server:call(ServerRef, {request, App, Request, Timeout}, infinity).

reply({ServerRef, From}, Reply) ->
	gen_server:call(ServerRef, {reply, From, Reply}, infinity).

subscribe(ServerRef, Topic) ->
	gen_server:call(ServerRef, {subscribe, Topic}).

publish(ServerRef, Topic, Event) ->
	gen_server:call(ServerRef, {publish, Topic, Event}).

unsubscribe(ServerRef, Topic) ->
	gen_server:call(ServerRef, {unsubscribe, Topic}).

tunnel(ServerRef, App, Timeout) ->
	gen_server:call(ServerRef, {tunnel, App, Timeout}).

close(ServerRef) ->
	gen_server:call(ServerRef, close).


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

%% Initializes the callback handler and connects to the local Iris relay node.
init({Port, App, Module, Args}) ->
  % Initialize the callback handler
	Result = Module:init(Args),
	case Result of
		{stop, _} -> Result;
		ignore    -> Result;
		_Other    ->
			% Initialize the Iris connection
			case iris_relay:connect(Port, App) of
				{ok, Conn} ->
					{ok, #state{
						conn       = Conn,
						hand_mod   = Module,
						hand_state = element(2, Result)
					}};
				{error, Reason} -> {stop, Reason}
			end
	end.

handle_call({broadcast, App, Message}, _From, State = #state{conn = Conn}) ->
	{reply, iris_relay:broadcast(Conn, App, Message), State};

handle_call({request, App, Request, Timeout}, _From, State = #state{conn = Conn}) ->
	{reply, iris_relay:request(Conn, App, Request, Timeout), State};

handle_call({reply, From, Reply}, _From, State = #state{conn = Conn}) ->
	{reply, iris_relay:reply(Conn, From, Reply), State};

handle_call({subscribe, Topic}, _From, State = #state{conn = Conn}) ->
	{reply, iris_relay:subscribe(Conn, Topic), State};

handle_call({publish, Topic, Event}, _From, State = #state{conn = Conn}) ->
	{reply, iris_relay:publish(Conn, Topic, Event), State};

handle_call({unsubscribe, Topic}, _From, State = #state{conn = Conn}) ->
	{reply, iris_relay:unsubscribe(Conn, Topic), State};

handle_call({tunnel, App, Timeout}, _From, State = #state{conn = Conn}) ->
	{reply, iris_relay:tunnel(Conn, App, Timeout), State};

handle_call(close, _From, State = #state{conn = Conn}) ->
	{stop, normal, iris_relay:close(Conn), State#state{conn = nil}}.

handle_info({broadcast, Message}, State = #state{hand_mod = Mod}) ->
	case Mod:handle_broadcast(Message, State#state.hand_state) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end;

handle_info({request, From, Request}, State = #state{hand_mod = Mod}) ->
	case Mod:handle_request(Request, {self(), From}, State#state.hand_state) of
		{reply, Reply, NewState} ->
			ok = iris_relay:reply(From, Reply),
			{noreply, State#state{hand_state = NewState}};
		{noreply, NewState} ->
			{noreply, State#state{hand_state = NewState}};
		{stop, Reason, Reply, NewState} ->
			ok = iris_relay:reply(From, Reply),
			{stop, Reason, State#state{hand_state = NewState}};
		{stop, Reason, NewState} ->
			{stop, Reason, State#state{hand_state = NewState}}
	end;

handle_info({publish, Topic, Event}, State = #state{hand_mod = Mod}) ->
	case Mod:handle_publish(Topic, Event, State#state.hand_state) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end;

handle_info({tunnel, Tunnel}, State = #state{hand_mod = Mod}) ->
	case Mod:handle_tunnel(Tunnel, State#state.hand_state) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end;

handle_info({drop, Reason}, State = #state{hand_mod = Mod}) ->
	case Mod:handle_drop(Reason, State#state.hand_state) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end.

terminate(Reason, State = #state{conn = Conn, hand_mod = Mod}) ->
	Mod:terminate(Reason, State#state.hand_state),
	case Conn of
		nil -> ok;
		_   -> iris_relay:close(Conn)
	end.

%% =============================================================================
%% Unused generic server methods
%% =============================================================================

code_change(_OldVsn, _State, _Extra) ->
	{error, unimplemented}.

handle_cast(_Request, State) ->
	{stop, unimplemented, State}.

