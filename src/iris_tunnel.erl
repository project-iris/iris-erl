%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% @private

-module(iris_tunnel).
-export([buffer/0, send/3, recv/2, close/1]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
	code_change/3]).

%% Iris to app buffer size for flow control.
-spec buffer() -> pos_integer().

buffer() -> 128.

%% =============================================================================
%% External API functions
%% =============================================================================

%% Forwards the message to be sent to the tunnel process.
-spec send(Tunnel :: pid(), Message :: binary(), Timeout :: timeout()) ->
	ok | {error, Reason :: atom()}.

send(Tunnel, Message, Timeout) ->
	gen_server:call(Tunnel, {send, Message, Timeout}, infinity).

%% Forwards the receive request to the tunnel process.
-spec recv(Tunnel :: pid(), Timeout :: timeout()) ->
	{ok, Message :: binary()} | {error, Reason :: atom()}.

recv(Tunnel, Timeout) ->
	gen_server:call(Tunnel, {recv, Timeout}, infinity).

%% Forwards the close request to the tunnel.
-spec close(Tunnel :: pid()) ->
	ok | {error, Reason :: atom()}.

close(Tunnel) ->
	gen_server:call(Tunnel, close, infinity).


%% =============================================================================
%% Generic server internal state
%% =============================================================================

-record(state, {
	tunid,     %% Tunnel identifier for traffic relay
	relay,     %% Message relay to the iris node

	itoaReady, %% Iris to application message buffer
	itoaTasks, %% Iris to application pending receive tasks

	atoiReady, %% Application to Iris allowance buffer
	atoiTasks, %% Application to Iris pending send tasks

	term       %% Termination flag to prevent new sends
}).


%% =============================================================================
%% Generic server callback methods
%% =============================================================================

%% Initializes the tunnel with the two asymmetric buffers.
init({Relay, TunId, Buffer}) ->
	{ok, #state{
		tunid     = TunId,
		relay     = Relay,
		itoaReady = [],
		itoaTasks = [],
		atoiReady = Buffer,
		atoiTasks = [],
		term      = false
	}}.

%% Forwards an outbound message to the remote endpoint of the relay. If the send
%% limit is reached, then the call is blocked and a coutdown timer started.
handle_call({send, _Message, _Timeout}, _From, State = #state{term = true}) ->
	{reply, {error, closed}, State};

handle_call({send, Message, Timeout}, From, State = #state{atoiReady = 0, atoiTasks = Pend}) ->
	Id = make_ref(),
	TRef = case Timeout of
		infinity -> nil;
		_Other ->
			{ok, Ref} = timer:send_after(Timeout, {timeout, recv, Id}),
			Ref
	end,
	Task = {Id, From, Message, TRef},
	{noreply, State#state{atoiTasks = [Task | Pend]}};

handle_call({send, Message, _Timeout}, _From, State = #state{atoiReady = Ready}) ->
	Res = iris_relay:tunnel_send(State#state.relay, State#state.tunid, Message),
	{reply, Res, State#state{atoiReady = Ready - 1}};

%% Retrieves an inbound message from the local buffer and acks remote endpoint.
%% If no message is available locally, a timer is started and the call blocks.
handle_call({recv, _Timeout}, _From, State = #state{itoaReady = [], term = true}) ->
	{reply, {error, closed}, State};

handle_call({recv, Timeout}, From, State = #state{itoaReady = [], itoaTasks = Pend}) ->
	Id = make_ref(),
	TRef = case Timeout of
		infinity -> nil;
		_Other ->
			{ok, Ref} = timer:send_after(Timeout, {timeout, recv, Id}),
			Ref
	end,
	Task = {Id, From, TRef},
	{noreply, State#state{itoaTasks = [Task | Pend]}};

handle_call({recv, _Timeout}, _From, State = #state{itoaReady = [Msg | Rest]}) ->
	case iris_relay:tunnel_ack(State#state.relay, State#state.tunid) of
		ok    -> {reply, {ok, Msg}, State#state{itoaReady = Rest}};
		Error -> {reply, Error, State}
	end;

%% Notifies the relay of the close request (which may or may not forward it to
%% the Iris node), and terminates the process.
handle_call(close, _From, State = #state{}) ->
	Res = case State#state.term of
		false -> iris_relay:tunnel_close(State#state.relay, State#state.tunid);
		true  -> ok
	end,
	{stop, normal, Res, State}.

%% Notifies the pending send of failure due to timeout. In the rare case of the
%% timeout happening right before the timer is canceled, the event is dropped.
handle_info({timeout, send, Id}, State = #state{atoiTasks = Pend}) ->
	case lists:keyfind(Id, 1, Pend) of
		false            -> ok;
		{Id, From, _, _} -> gen_server:reply(From, {error, timeout})
	end,
	{noreply, State#state{atoiTasks = lists:keydelete(Id, 1, Pend)}};

%% Notifies the pending recv of failure due to timeout. In the rare case of the
%% timeout happening right before the timer is canceled, the event is dropped.
handle_info({timeout, recv, Id}, State = #state{itoaTasks = Pend}) ->
	case lists:keyfind(Id, 1, Pend) of
		false         -> ok;
		{Id, From, _} -> gen_server:reply(From, {error, timeout})
	end,
	{noreply, State#state{itoaTasks = lists:keydelete(Id, 1, Pend)}};

%% Accepts an inbound data packet, and either delivers it to a pending receive
%% or buffers it locally.
handle_info({data, Message}, State = #state{itoaTasks = [], itoaReady = Ready}) ->
	{noreply, State#state{itoaReady = Ready ++ [Message]}};

handle_info({data, Message}, State = #state{itoaTasks = [Task | Rest]}) ->
	{_, From, TRef} = Task,
	case TRef of
		nil    -> ok;
		_Other ->	{ok, cancel} = timer:cancel(TRef)
	end,
	case iris_relay:tunnel_ack(State#state.relay, State#state.tunid) of
		ok    -> gen_server:reply(From, {ok, Message});
		Error -> gen_server:reply(From, Error)
	end,
	{noreply, State#state{itoaTasks = Rest}};

%% Acks a forwarded message as sent, allowing either the next pending packet to
%% be forwarded to the relay, or a new send slot added.
handle_info(ack, State = #state{atoiTasks = [], atoiReady = Ready}) ->
	{noreply, State#state{atoiReady = Ready + 1}};

handle_info(ack, State = #state{atoiTasks = [Task | Rest]}) ->
	{_, From, Message, TRef} = Task,
	case TRef of
		nil    -> ok;
		_Other ->	{ok, cancel} = timer:cancel(TRef)
	end,
	Res = iris_relay:tunnel_send(State#state.relay, State#state.tunid, Message),
	gen_server:reply(From, Res),
	{noreply, State#state{atoiTasks = Rest}};

%% Sets the tunnel's closed flag, preventing new sends from going through. Any
%% data already received will be available for extraction before any error is
%% returned.
handle_info(close, State) ->
	% Notify all pending receives of the closure
	lists:foreach(fun({_, From, TRef}) ->
		case TRef of
			nil    -> ok;
			_Other ->	{ok, cancel} = timer:cancel(TRef)
		end,
		gen_server:reply(From, {error, closed})
	end, State#state.itoaTasks),

	% Notify all pending sends of the closure
	lists:foreach(fun({_, From, _Message, TRef}) ->
		case TRef of
			nil    -> ok;
			_Other ->	{ok, cancel} = timer:cancel(TRef)
		end,
		gen_server:reply(From, {error, closed})
	end, State#state.atoiTasks),

	% Clean out the pending queue and set the term flag
	{noreply, State#state{itoaTasks = [], term = true}}.

%% Cleanup method, does nothing really.
terminate(_Reason, _State) -> ok.


%% =============================================================================
%% Unused generic server methods
%% =============================================================================

code_change(_OldVsn, _State, _Extra) ->
	{error, unimplemented}.

handle_cast(_Request, State) ->
	{stop, unimplemented, State}.
