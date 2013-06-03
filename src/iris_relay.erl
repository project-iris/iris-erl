%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

-module(iris_relay).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export([connect/2, broadcast/3, close/1]).

-record(state, {
	socket,
	closer
}).

%% =============================================================================
%% External API functions
%% =============================================================================

%% Starts the gen_server responsible for the relay connection.
connect(Port, App) ->
	gen_server:start_link(?MODULE, {Port, App, self()}, []).

%% Forwards a broadcasted message for relaying.
broadcast(Connection, App, Message) ->
	gen_server:call(Connection, {broadcast, App, Message}, infinity).

%% Notifies the relay server of a gracefull close request.
close(Connection) ->
	gen_server:call(Connection, close, infinity).

%% =============================================================================
%% Generic server callback methods
%% =============================================================================

%% Connects to the locally running iris node and initiates the connection.
init({Port, App, Handler}) ->
	% Open the TCP connection
	case gen_tcp:connect({127,0,0,1}, Port, [{active, false}, binary]) of
		{ok, Socket} ->
			% Send the init packet
			case iris_proto:sendInit(Socket, App) of
				ok ->
					% Wait for init confirmation
					case iris_proto:procInit(Socket) of
						ok ->
							% Spawn the receiver thread and return
							process_flag(trap_exit, true),
							spawn_link(iris_proto, process, [Socket, self(), Handler]),
							{ok, #state{socket = Socket, closer = nil}};
						{error, Reason} -> {stop, Reason}
					end;
				{error, Reason} -> {stop, Reason}
			end;
		{error, Reason} -> {stop, Reason}
	end.

%% Relays a message to the Iris node for broadcasting.
handle_call({broadcast, App, Message}, _From, State = #state{socket = Socket}) ->
	{reply, iris_proto:sendBroadcast(Socket, App, Message), State};

%% Sends a gracefull close request to the relay. The reply should arrive async.
handle_call(close, From, State = #state{socket = Socket}) ->
	case iris_proto:sendClose(Socket) of
		ok                      -> {noreply, State#state{closer = From}};
		Error = {error, Reason} -> {stop, Reason, Error, State}
	end;

%% @doc Handles a subscription event. Depending on the Return flag, the specified queue might be set as the return queue
%%      for requests (basically if a sync call is made, that's where the result will arrive).
%% @end
handle_call(Message, From, State) ->
	io:format("Unknown call: ~p~p~p~n", [Message, From, State]),
	{reply, ok, State}.

handle_cast(stop, State) ->
	{stop, normal, State}.

%% Handles the termination of the receiver thread: either returns a clean exit
%% or notifies the handler of a drop.
handle_info({'EXIT', _Pid, Reason}, State) ->
	case State#state.closer of
		nil ->
			{stop, Reason, State};
		Pid ->
			gen_server:reply(Pid, ok),
			{stop, normal, State}
	end.

%% Final cleanup, close up the relay link.
terminate(_Reason, State) ->
	gen_tcp:close(State#state.socket).
