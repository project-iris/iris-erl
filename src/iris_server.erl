%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

%% @doc
%% A behavior module for implementing
%% @end

-module(iris_server).
-export([start/4, start_link/4, stop/1]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
	code_change/3]).

%% =============================================================================
%% Iris server behaviour definitions
%% =============================================================================

-callback init(Args :: term()) ->
	{ok, State :: term()} | ignore | {stop, Reason :: term()}.

-callback handle_broadcast(Message :: binary(), State :: term(), Link :: iris:connection()) ->
	{noreply, NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback handle_request(Request :: binary(), From :: iris:sender(), State :: term(), Link :: iris:connection()) ->
	{reply, Reply :: binary(), NewState :: term()} | {noreply, NewState :: term()} |
	{stop, Reply :: binary(), Reason :: term(), NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback handle_publish(Topic :: string(), Event :: binary(), State :: term(), Link :: iris:connection()) ->
	{noreply, NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback handle_tunnel(Tunnel :: iris:tunnel(), State :: term(), Link :: iris:connection()) ->
	{noreply, NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback handle_drop(Reason :: term(), State :: term()) ->
	{noreply, NewState :: term()} | {stop, Reason :: term(), NewState :: term()}.

-callback terminate(Reason :: term(), State :: term()) ->
	no_return().


%% =============================================================================
%% External API functions
%% =============================================================================

%% @doc Creates and starts an Iris server process.
%%
%%      The server will connect to the locally running Iris relay, and receive
%%      all the inbound network events, conforming to the iris_server behavior.
%%
%% @spec (Port, App, Module, Args) -> {ok, Server, Connection} | {error, Reason}
%%      Port       = pos_integer()
%%      App        = string()
%%      Module     = atom()
%%      Args       = term()
%%      Server     = pid()
%%      Connection = connection()
%%      Reason     = atom()
%% @end
-spec start(Port :: pos_integer(), App :: string(), Module :: atom(), Args :: term()) ->
	{ok, Server :: pid(), Connection :: iris:connection()} | {error, Reason :: atom()}.

start(Port, App, Module, Args) ->
	case gen_server:start(?MODULE, {Port, App, Module, Args}, []) of
		{ok, Pid} ->
			{ok, Link} = gen_server:call(Pid, link, infinity),
			{ok, Pid, Link};
		Other -> Other
	end.

%% @doc Creates and starts an Iris server process, linked to the caller.
%%
%%      The server will connect to the locally running Iris relay, and receive
%%      all the inbound network events, conforming to the iris_server behavior.
%%
%% @spec (Port, App, Module, Args) -> {ok, Server, Connection} | {error, Reason}
%%      Port       = pos_integer()
%%      App        = string()
%%      Module     = atom()
%%      Args       = term()
%%      Server     = pid()
%%      Connection = connection()
%%      Reason     = atom()
%% @end
-spec start_link(Port :: pos_integer(), App :: string(), Module :: atom(), Args :: term()) ->
	{ok, Server :: pid(), Connection :: iris:connection()} | {error, Reason :: atom()}.

start_link(Port, App, Module, Args) ->
	case gen_server:start_link(?MODULE, {Port, App, Module, Args}, []) of
		{ok, Pid} ->
			{ok, Link} = gen_server:call(Pid, link, infinity),
			{ok, Pid, Link};
		Other -> Other
	end.

%% @doc Gracefully terminates the server process.
%%
%% @spec (Server) -> ok | {error, Reason}
%%      Server     = pid()
%%      Reason     = atom()
%% @end
-spec stop(Server :: pid()) ->
	ok | {error, Reason :: atom()}.

stop(ServerRef) ->
	gen_server:call(ServerRef, stop).


%% =============================================================================
%% Generic server internal state
%% =============================================================================

-record(state, {
	link,       %% High level Iris connection
  hand_mod,   %% Handler callback module
  hand_state  %% Handler internal state
}).


%% =============================================================================
%% Generic server callback methods
%% =============================================================================

%% Initializes the callback handler and connects to the local Iris relay node.
%% @private
init({Port, App, Module, Args}) ->
  % Initialize the callback handler
	Result = Module:init(Args),
	case Result of
		{stop, _} -> Result;
		ignore    -> Result;
		_Other    ->
			% Initialize the Iris connection
			case iris:connect(Port, App) of
				{ok, Link} ->
					{ok, #state{
						link       = Link,
						hand_mod   = Module,
						hand_state = element(2, Result)
					}};
				{error, Reason} -> {stop, Reason}
			end
	end.

%% @private
%% Returns the Iris connection. Used only during server startup.
handle_call(link, _From, State = #state{link = Link}) ->
	{reply, {ok, Link}, State};

%% Closes the Iris connection, returning the result to the caller.
handle_call(stop, _From, State = #state{link = Link}) ->
	{stop, normal, iris:close(Link), State#state{link = nil}}.

%% @private
%% Delivers a broadcast message to the callback and processes the result.
handle_info({broadcast, Message}, State = #state{link = Link, hand_mod = Mod}) ->
	case Mod:handle_broadcast(Message, State#state.hand_state, Link) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end;

%% Delivers a request to the callback and processes the result.
handle_info({request, From, Request}, State = #state{link = Link, hand_mod = Mod}) ->
	case Mod:handle_request(Request, From, State#state.hand_state, Link) of
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
handle_info({publish, Topic, Event}, State = #state{link = Link, hand_mod = Mod}) ->
	case Mod:handle_publish(Topic, Event, State#state.hand_state, Link) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end;

%% Delivers an inbound tunnel to the callback and processes the result.
handle_info({tunnel, Tunnel}, State = #state{link = Link, hand_mod = Mod}) ->
	case Mod:handle_tunnel(Tunnel, State#state.hand_state, Link) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end;

%% Notifies the callback of the connection drop and processes the result.
handle_info({drop, Reason}, State = #state{link = Link, hand_mod = Mod}) ->
	case Mod:handle_drop(Reason, State#state.hand_state, Link) of
		{noreply, NewState}      -> {noreply, State#state{hand_state = NewState}};
		{stop, Reason, NewState} -> {stop, Reason, State#state{hand_state = NewState}}
	end.

%% @private
%% Notifies the callback of the termination and closes the link if still up.
terminate(Reason, State = #state{link = Link, hand_mod = Mod}) ->
	Mod:terminate(Reason, State#state.hand_state),
	case Link of
		nil -> ok;
		_   -> iris:close(Link)
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
