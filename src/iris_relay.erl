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

-export([connect/2, broadcast/3, request/4, reply/2, close/1]).

%% =============================================================================
%% External API functions
%% =============================================================================

%% Starts the gen_server responsible for the relay connection.
connect(Port, App) ->
	gen_server:start_link(?MODULE, {Port, App, self()}, []).

%% Forwards a broadcasted message for relaying.
broadcast(Connection, App, Message) ->
	gen_server:call(Connection, {broadcast, App, Message}, infinity).

%% Forwards the request to the relay. Timeouts are handled relay side.
request(Connection, App, Request, Timeout) ->
	gen_server:call(Connection, {request, App, Request, Timeout}, infinity).

%% Forwards an async reply to the relay to be sent back to the caller.
reply({Connection, RequestId}, Reply) ->
	gen_server:call(Connection, {reply, RequestId, Reply}, infinity).

%% Notifies the relay server of a gracefull close request.
close(Connection) ->
	gen_server:call(Connection, close, infinity).


%% =============================================================================
%% Generic server internal state
%% =============================================================================

-record(state, {
	sock,     %% Network connection to the iris node

	reqIdx,   %% Index to assign the next request
	reqPend,  %% Active requests waiting for a reply

	closer
}).


%% =============================================================================
%% Generic server callback methods
%% =============================================================================

%% Connects to the locally running iris node and initiates the connection.
init({Port, App, Handler}) ->
	% Open the TCP connection
	case gen_tcp:connect({127,0,0,1}, Port, [{active, false}, binary]) of
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
							ReqPend = ets:new(requests, [set, private]),
							{ok, #state{
								sock    = Sock,
								reqIdx  = 0,
								reqPend = ReqPend,
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

%% Relays a request to the Iris node, waiting async with a result channel for the reply.
handle_call({request, App, Request, Timeout}, From, State = #state{sock = Sock}) ->
	% Create a reply channel for the results
	ReqId = State#state.reqIdx,
	ets:insert(State#state.reqPend, {ReqId, From}),
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

%% Sends a gracefull close request to the relay. The reply should arrive async.
handle_call(close, From, State = #state{sock = Sock}) ->
	case iris_proto:sendClose(Sock) of
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

%% Delivers a reply to a pending request.
handle_info({reply, ReqId, Reply}, State) ->
	% Fetch the result channel and remove from state
	{ReqId, Pending} = hd(ets:lookup(State#state.reqPend, ReqId)),
	ets:delete(State#state.reqPend, ReqId),

	% Reply to the pending process and return
	gen_server:reply(Pending, Reply),
	{noreply, State};

%% Handles the termination of the receiver thread: either returns a clean exit
%% or notifies the handler of a drop.
handle_info({'EXIT', _Pid, Reason}, State) ->
	% Notify all pending requests of the failure
	lists:foreach(fun({_ReqId, Pid}) ->
		gen_server:reply(Pid, {error, terminating})
	end, ets:tab2list(State#state.reqPend)),

	% Terminate, notifying the closer if needed
	case State#state.closer of
		nil ->
			{stop, Reason, State};
		Pid ->
			gen_server:reply(Pid, ok),
			{stop, normal, State}
	end.

%% Final cleanup, close up the relay link.
terminate(_Reason, State) ->
	gen_tcp:close(State#state.sock).
