%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% @private

-module(iris_tunnel).
-export([send/3, recv/2, close/1]).
-export([start_link/2, start_link/3, finalize/2, handle_allowance/2,
	handle_transfer/3, handle_close/2]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
	code_change/3]).


%% =============================================================================
%% External API functions
%% =============================================================================

%% Forwards the message to be sent to the tunnel process.
-spec send(Tunnel :: pid(), Message :: binary(), Timeout :: timeout()) ->
	ok | {error, Reason :: atom()}.

send(Tunnel, Message, Timeout) ->
	gen_server:call(Tunnel, {schedule_send, Message, Timeout}, infinity).


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
%% Internal API functions
%% =============================================================================

-spec start_link(Id :: non_neg_integer(), Logger :: iris_logger:logger())
	-> {ok, Server :: pid()} | {error, Reason :: term()}.

start_link(Id, Logger) ->
	gen_server:start_link(?MODULE, {self(), Id, 0, Logger}, []).


-spec start_link(Id :: non_neg_integer(), ChunkLimit :: pos_integer(),
	Logger :: iris_logger:logger()) -> {ok, Server :: pid()} | {error, Reason :: term()}.

start_link(Id, ChunkLimit, Logger) ->
	gen_server:start_link(?MODULE, {self(), Id, ChunkLimit, Logger}, []).


-spec finalize(Tunnel :: pid(), ChunkLimit :: pos_integer()) -> ok.

finalize(Tunnel, ChunkLimit) ->
	gen_server:call(Tunnel, {finalize, ChunkLimit}).


-spec potentially_send() -> ok.

potentially_send() ->
 ok = gen_server:cast(self(), potentially_send).

%% =============================================================================
%% Internal API callback functions
%% =============================================================================

%% @private
%% Schedules an application allowance for the service handler to process.
-spec handle_allowance(Tunnel :: pid(), Space :: pos_integer()) -> ok.

handle_allowance(Tunnel, Space) ->
  ok = gen_server:cast(Tunnel, {handle_allowance, Space}).


%% @private
%% Schedules an application transfer for the service handler to process.
-spec handle_transfer(Tunnel :: pid(), SizeOrCont :: non_neg_integer(),
	Payload :: binary()) -> ok.

handle_transfer(Tunnel, SizeOrCont, Payload) ->
  ok = gen_server:cast(Tunnel, {handle_transfer, SizeOrCont, Payload}).


%% @private
%% Schedules an application transfer for the service handler to process.
-spec handle_close(Tunnel :: pid(), Reason :: string()) -> ok.

handle_close(Tunnel, Reason) ->
  ok = gen_server:cast(Tunnel, {handle_close, Reason}).


%% =============================================================================
%% Generic server internal state
%% =============================================================================

-record(state, {
	id,        %% Tunnel identifier for de/multiplexing
	conn,      %% Connection to the local relay

	chunkLimit, %% Maximum length of a data payload
	chunkBuf,   %% Current message being assembled

	itoaBuf,   %% Iris to application message buffer
	itoaTasks, %% Iris to application pending receive tasks

	atoiSpace, %% Application to Iris space allowance
	atoiTasks, %% Application to Iris pending send tasks

	term,      %% Termination flag to prevent new sends
  closer,    %% Processes waiting for the close ack
  stat,      %% Failure reason, if any received
  logger     %% Logger with connection and tunnel ids injected
}).


%% =============================================================================
%% Generic server callback methods
%% =============================================================================

%% Initializes the tunnel with the two asymmetric buffers.
init({Conn, Id, ChunkLimit, Logger}) ->
	{ok, #state{
		id         = Id,
		conn       = Conn,
		chunkLimit = ChunkLimit,
		chunkBuf   = <<>>,
		itoaBuf    = [],
		itoaTasks  = [],
		atoiSpace  = 0,
		atoiTasks  = [],
		term       = false,
    closer     = nil,
    stat       = "",
    logger     = Logger
	}}.


handle_call({finalize, ChunkLimit}, _From, State) ->
	{reply, ok, State#state{chunkLimit = ChunkLimit}};

%% Forwards an outbound message to the remote endpoint of the conn. If the send
%% limit is reached, then the call is blocked and a countdown timer started.
handle_call({schedule_send, _Payload, _Timeout}, _From, State = #state{term = true}) ->
	{reply, {error, closed}, State};

handle_call({schedule_send, Payload, Timeout}, From, State = #state{chunkLimit = Limit, atoiTasks = Pend}) ->
	% Start a timer for the operation to complete
	Id = make_ref(),
	TRef = case Timeout of
		infinity -> nil;
		_Other ->
			{ok, Ref} = timer:send_after(Timeout, {timeout, send, Id}),
			Ref
	end,

	% Split the message into a list of tasks
	Size   = byte_size(Payload),
	Pieces = (Size + Limit - 1) div Limit,
	Tasks  = lists:foldl(fun(Index, Tasks) ->
		SizeOrCont = case Index of
			1 -> Size;
			_ -> 0
		end,
		Chunk = binary:part(Payload, (Index - 1) * Limit, erlang:min(Index * Limit, Size)),
		Task = case Index < Pieces of
			true  -> {Id, SizeOrCont, Chunk};
			false -> {Id, SizeOrCont, Chunk, From, TRef}
		end,
		[Task | Tasks]
	end, [], lists:seq(1, Pieces)),

	% Schedule the pieces and initiate a potential send
	ok = potentially_send(),
	{noreply, State#state{atoiTasks = lists:append(Pend, lists:reverse(Tasks))}};

%% Retrieves an inbound message from the local buffer and acks remote endpoint.
%% If no message is available locally, a timer is started and the call blocks.
handle_call({recv, _Timeout}, _From, State = #state{itoaBuf = [], term = true}) ->
	{reply, {error, closed}, State};

handle_call({recv, Timeout}, From, State = #state{itoaBuf = [], itoaTasks = Pend}) ->
	Id = make_ref(),
	TRef = case Timeout of
		infinity -> nil;
		_Other ->
			{ok, Ref} = timer:send_after(Timeout, {timeout, recv, Id}),
			Ref
	end,
	Task = {Id, From, TRef},
	{noreply, State#state{itoaTasks = [Task | Pend]}};

handle_call({recv, _Timeout}, _From, State = #state{itoaBuf = [Msg | Rest]}) ->
	case iris_conn:tunnel_allowance(State#state.conn, State#state.id, byte_size(Msg)) of
		ok    -> {reply, {ok, Msg}, State#state{itoaBuf = Rest}};
		Error -> {reply, Error, State}
	end;

%% Notifies the conn of the close request (which may or may not forward it to
%% the Iris node), and terminates the process.
handle_call(close, From, State = #state{}) ->
	case State#state.term of
    true  -> {stop, normal, State#state.stat, State};
		false ->
			iris_logger:info(State#state.logger, "closing tunnel"),
      ok = iris_conn:tunnel_close(State#state.conn, State#state.id),
      {noreply, State#state{closer = From}}
	end.

%% If there is enough allowance and data schedules, sends a chunk to the relay.
handle_cast(potentially_send, State = #state{atoiTasks = []}) ->
	{noreply, State};

handle_cast(potentially_send, State = #state{atoiTasks = [Task | Rest], atoiSpace = Allowance}) ->
	Size = byte_size(erlang:element(3, Task)),
	case Allowance >= Size of
		true ->
			SizeOrCont = erlang:element(2, Task),
			Chunk      = erlang:element(3, Task),
			ok = iris_conn:tunnel_send(State#state.conn, State#state.id, SizeOrCont, Chunk),
			ok = potentially_send(),
			case Task of
				{_Id, _SizeOrCont, _Chunk}             -> ok;
				{_Id, _SizeOrCont, _Chunk, From, TRef} ->
					case TRef of
						nil -> ok;
						_   -> {ok, cancel} = timer:cancel(TRef)
					end,
					gen_server:reply(From, ok)
			end,
			{noreply, State#state{atoiTasks = Rest}};
		false -> {noreply, State}
	end;

%% Increments the available outbound space and invokes a potential send.
handle_cast({handle_allowance, Space}, State = #state{atoiSpace = Allowance}) ->
	ok = potentially_send(),
	{noreply, State#state{atoiSpace = Allowance + Space}};

%% Accepts an inbound data packet, and either delivers it to a pending receive
%% or buffers it locally.
handle_cast({handle_transfer, SizeOrCont, Payload}, State = #state{itoaTasks = [], itoaBuf = Ready}) ->
	{noreply, State#state{itoaBuf = Ready ++ [Payload]}};

handle_cast({handle_transfer, SizeOrCont, Payload}, State = #state{itoaTasks = [Task | Rest]}) ->
	{_, From, TRef} = Task,
	case TRef of
		nil    -> ok;
		_Other ->	{ok, cancel} = timer:cancel(TRef)
	end,
	case iris_conn:tunnel_allowance(State#state.conn, State#state.id, byte_size(Payload)) of
		ok    -> gen_server:reply(From, {ok, Payload});
		Error -> gen_server:reply(From, Error)
	end,
	{noreply, State#state{itoaTasks = Rest}};

%% Handles the graceful remote closure of the tunnel.
handle_cast({handle_close, Reason}, State = #state{conn = Conn, id = Id}) ->
  Status = case Reason of
    [] ->
    	iris_logger:info(State#state.logger, "tunnel closed gracefully"),
    	ok;
    _  ->
    	iris_logger:info(State#state.logger, "tunnel dropped", [{reason, Reason}]),
    	{error, Reason}
  end,

  % Notify all pending receives of the closure
  lists:foreach(fun({_, From, TRef}) ->
    case TRef of
      nil    -> ok;
      _Other -> {ok, cancel} = timer:cancel(TRef)
    end,
    gen_server:reply(From, {error, closed})
  end, State#state.itoaTasks),

  % Notify all pending sends of the closure
  lists:foreach(fun(Task) ->
    case Task of
      {_Id, _SizeOrCont, _Chunk, From, TRef} ->
        case TRef of
          nil    -> ok;
          _Other -> {ok, cancel} = timer:cancel(TRef)
        end,
        gen_server:reply(From, {error, closed});
      {_Id, _SizeOrCont, _Chunk} -> ok
    end
  end, State#state.atoiTasks),

  % Notify the connection to dump the tunnel
  ok = iris_conn:handle_tunnel_close(Conn, Id),

  % Notify the closer (if any) of the termination
  case State#state.closer of
    nil  -> {noreply, State#state{itoaTasks = [], term = true, stat = Status}};
    From ->
      gen_server:reply(From, Status),
      {stop, normal, State#state{itoaTasks = [], term = true, stat = Reason}}
  end.


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
	{noreply, State#state{itoaTasks = lists:keydelete(Id, 1, Pend)}}.

%% Cleanup method, does nothing really.
terminate(_Reason, _State) -> ok.


%% =============================================================================
%% Unused generic server methods
%% =============================================================================

code_change(_OldVsn, _State, _Extra) ->
	{error, unimplemented}.
