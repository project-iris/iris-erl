%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

-module(iris_tunnel).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2]).
-export([buffer/0, send/3, recv/2, close/1]).

%% Iris to app buffer size for flow control.
buffer() -> 128.

%% =============================================================================
%% External API functions
%% =============================================================================

%% @doc Sends a message over the tunnel to the remote pair, blocking until the
%%      local relay node receives the message.
%%
%% @spec (Tunnel, Message, Timeout) -> ok | {error, Reason}
%%      Tunnel  = pid()
%%      Message = binary()
%%      Timeout = int()>0
%%      Reason  = term()
%% @end
send(Tunnel, Message, Timeout) ->
	gen_server:call(Tunnel, {send, Message, Timeout}, infinity).

%% @doc Retrieves a message from the tunnel, blocking until one is available.
%%
%% @spec (Tunnel, Timeout) -> {ok, Message} | {error, Reason}
%%      Tunnel  = pid()
%%      Timeout = int()>0
%%      Message = binary()
%%      Reason  = term()
%% @end
recv(Tunnel, Timeout) ->
	gen_server:call(Tunnel, {recv, Timeout}, infinity).

%% @doc Closes the tunnel between the pair. Any blocked read and write operation
%%      will terminate with a failure.
%%
%% @spec (Tunnel) -> ok | {error, Reason}
%%      Tunnel  = pid()
%%      Reason  = term()
%% @end
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
	{ok, TRef} = timer:send_after(Timeout, {timeout, send, Id}),
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
	{ok, TRef} = timer:send_after(Timeout, {timeout, recv, Id}),
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
	Res = iris_relay:tunnel_close(State#state.relay, State#state.tunid),
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
	{ok, cancel} = timer:cancel(TRef),
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
	{ok, cancel} = timer:cancel(TRef),
	Res = iris_relay:tunnel_send(State#state.relay, State#state.tunid, Message),
	gen_server:reply(From, Res),
	{noreply, State#state{atoiTasks = Rest}};

%% Sets the tunnel's closed flag, preventing new sends from going through. Any
%% data already received will be available for extraction before any error is
%% returned.
handle_info(close, State) ->
	{noreply, State#state{term = true}}.

%% Cleanup method, does nothing really.
terminate(_Reason, _State) -> ok.
