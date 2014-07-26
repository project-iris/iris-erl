%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% @doc Module responsible for communicating through an ordered and throttled
%%      data stream,
%% @end

-module(iris_tunnel).
-export([send/3, recv/2, close/1, logger/1]).
-export_type([tunnel/0]).

-export([start_link/2, start_link/3, finalize/2, handle_allowance/2,
	handle_transfer/3, handle_close/2]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_info/2, terminate/2, handle_cast/2,
	code_change/3]).


-type tunnel() :: pid(). %% Communication stream between the local application
                         %% and a remote endpoint.


%% =============================================================================
%% External API functions
%% =============================================================================

%% @doc Sends a message over the tunnel to the remote pair, blocking until the
%%      local Iris node receives the message or the operation times out.
%%
%%      Infinite blocking is supported with by setting the timeout to infinity.
%%
%% @spec (Tunnel, Message, Timeout) -> ok | {error, Reason}
%%      Tunnel  = iris_tunnel:tunnel()
%%      Message = binary()
%%      Timeout = timeout()
%%      Reason  = timeout | atom()
%% @end
-spec send(Tunnel :: iris_tunnel:tunnel(), Message :: binary(), Timeout :: timeout()) ->
	ok | {error, Reason :: atom()}.

send(Tunnel, Message, Timeout) ->
	gen_server:call(Tunnel, {schedule_send, Message, Timeout}, infinity).


%% @doc Retrieves a message from the tunnel, blocking until one is available or
%%      the operation times out.
%%
%%      Infinite blocking is supported with by setting the timeout to infinity.
%%
%% @spec (Tunnel, Timeout) -> {ok, Message} | {error, Reason}
%%      Tunnel  = iris_tunnel:tunnel()
%%      Timeout = timeout()
%%      Message = binary()
%%      Reason  = timeout | atom()
%% @end
-spec recv(Tunnel :: iris_tunnel:tunnel(), Timeout :: timeout()) ->
	{ok, Message :: binary()} | {error, Reason :: atom()}.

recv(Tunnel, Timeout) ->
	gen_server:call(Tunnel, {schedule_recv, Timeout}, infinity).


%% @doc Closes the tunnel between the pair. Any blocked read and write operation
%%      will terminate with a failure.
%%
%%      The method blocks until the local relay node acknowledges the tear-down.
%%
%% @spec (Tunnel) -> ok | {error, Reason}
%%      Tunnel = iris_tunnel:tunnel()
%%      Reason = atom()
%% @end
-spec close(Tunnel :: iris_tunnel:tunnel()) ->
	ok | {error, Reason :: atom()}.

close(Tunnel) ->
	gen_server:call(Tunnel, close, infinity).


%% @doc Retrieves the contextual logger associated with the tunnel.
%%
%% @spec (Tunnel) -> Logger
%%      Tunnel = pid()
%%      Logger = iris_logger:logger()
%% @end
-spec logger(Tunnel :: iris_tunnel:tunnel()) -> Logger :: iris_logger:logger().

logger(Tunnel) ->
	gen_server:call(Tunnel, {logger}, infinity).


%% =============================================================================
%% Internal API functions
%% =============================================================================

%% @private
-spec start_link(Id :: non_neg_integer(), Logger :: iris_logger:logger())
	-> {ok, Server :: pid()} | {error, Reason :: term()}.

start_link(Id, Logger) ->
	gen_server:start_link(?MODULE, {self(), Id, 0, Logger}, []).


%% @private
-spec start_link(Id :: non_neg_integer(), ChunkLimit :: pos_integer(),
	Logger :: iris_logger:logger()) -> {ok, Server :: pid()} | {error, Reason :: term()}.

start_link(Id, ChunkLimit, Logger) ->
	gen_server:start_link(?MODULE, {self(), Id, ChunkLimit, Logger}, []).


%% @private
-spec finalize(Tunnel :: iris_tunnel:tunnel(), Result :: {ok, ChunkLimit :: pos_integer()} |
	{error, Reason :: term()}) -> ok.

finalize(Tunnel, Result) ->
	gen_server:call(Tunnel, {finalize, Result}).


%% =============================================================================
%% Internal API callback functions
%% =============================================================================

%% @private
%% Schedules an application allowance for the service handler to process.
-spec handle_allowance(Tunnel :: iris_tunnel:tunnel(), Space :: pos_integer()) -> ok.

handle_allowance(Tunnel, Space) ->
  ok = gen_server:cast(Tunnel, {handle_allowance, Space}).


%% @private
%% Schedules an application transfer for the service handler to process.
-spec handle_transfer(Tunnel :: iris_tunnel:tunnel(), SizeOrCont :: non_neg_integer(),
	Payload :: binary()) -> ok.

handle_transfer(Tunnel, SizeOrCont, Payload) ->
  ok = gen_server:cast(Tunnel, {handle_transfer, SizeOrCont, Payload}).


%% @private
%% Schedules an application transfer for the service handler to process.
-spec handle_close(Tunnel :: iris_tunnel:tunnel(), Reason :: string()) -> ok.

handle_close(Tunnel, Reason) ->
  ok = gen_server:cast(Tunnel, {handle_close, Reason}).


%% =============================================================================
%% Generic server internal state
%% =============================================================================

-record(state, {
	id,         %% Tunnel identifier for de/multiplexing
	conn,       %% Connection to the local relay

	chunkLimit, %% Maximum length of a data payload
	chunkBuf,   %% Current message being assembled
	chunkSize,  %% Current size of the message being assembled
	chunkTotal, %% Total size of the message to assemble

	itoaBuf,    %% Iris to application message buffer
	itoaPend,   %% Iris to application pending receive

	atoiSpace,  %% Application to Iris space allowance
	atoiPend,   %% Application to Iris pending send

	term,       %% Termination flag to prevent new sends
  closer,     %% Processes waiting for the close ack
  stat,       %% Failure reason, if any received
  logger      %% Logger with connection and tunnel ids injected
}).


%% =============================================================================
%% Generic server callback methods
%% =============================================================================

%% @private
%% Initializes the tunnel with the two asymmetric buffers.
init({Conn, Id, ChunkLimit, Logger}) ->
	{ok, #state{
		id         = Id,
		conn       = Conn,
		chunkLimit = ChunkLimit,
		chunkBuf   = [],
		chunkSize  = 0,
		chunkTotal = 0,
		itoaBuf    = [],
		itoaPend   = nil,
		atoiSpace  = 0,
		atoiPend   = nil,
		term       = false,
    closer     = nil,
    stat       = "",
    logger     = Logger
	}}.


%% @private
handle_call({finalize, {ok, ChunkLimit}}, _From, State) ->
	{reply, ok, State#state{chunkLimit = ChunkLimit}};


handle_call({finalize, {error, _Reason}}, _From, State) ->
	{stop, normal, ok, State};


%% Forwards an outbound message to the remote endpoint of the conn. If the send
%% limit is reached, then the call is blocked and a countdown timer started.
handle_call({schedule_send, _Payload, _Timeout}, _From, State = #state{term = true}) ->
	{reply, {error, closed}, State};

handle_call({schedule_send, Payload, Timeout}, From, State = #state{atoiPend = nil}) ->
	% Start a timer for the operation to complete
	TRef = case Timeout of
		infinity -> nil;
		_Other ->
			{ok, Ref} = timer:send_after(Timeout, send_timeout),
			Ref
	end,

	% Create the pending send task and potentially send a chunk
	Task = {Payload, 0, From, TRef},
	{noreply, potentially_send(State#state{atoiPend = Task})};

%% Retrieves an inbound message from the local buffer and acks remote endpoint.
%% If no message is available locally, a timer is started and the call blocks.
handle_call({schedule_recv, _Timeout}, _From, State = #state{itoaBuf = [], term = true}) ->
	{reply, {error, closed}, State};

handle_call({schedule_recv, Timeout}, From, State = #state{itoaPend = nil}) ->
	TRef = case Timeout of
		infinity -> nil;
		_Other ->
			{ok, Ref} = timer:send_after(Timeout, recv_timeout),
			Ref
	end,

	% Create a pending receive task and potentially receive a message
	Task = {From, TRef},
	{noreply, potentially_recv(State#state{itoaPend = Task})};

%% Notifies the conn of the close request (which may or may not forward it to
%% the Iris node), and terminates the process.
handle_call(close, From, State = #state{}) ->
	case State#state.term of
    true  -> {stop, normal, State#state.stat, State};
		false ->
			iris_logger:info(State#state.logger, "closing tunnel"),
      ok = iris_conn:tunnel_close(State#state.conn, State#state.id),
      {noreply, State#state{closer = From}}
	end;

%% Retrieves the logger associated with the server.
handle_call({logger}, _From, State = #state{logger = Logger}) ->
	{reply, Logger, State}.

%% @private
%% Increments the available outbound space and invokes a potential send.
handle_cast({handle_allowance, Space}, State = #state{atoiSpace = Allowance}) ->
	{noreply, potentially_send(State#state{atoiSpace = Allowance + Space})};

%% Accepts an inbound data packet, and either delivers it to a pending receive
%% or buffers it locally.
handle_cast({handle_transfer, SizeOrCont, Payload}, State = #state{itoaBuf = Queue}) ->
  % If a new message is arriving, dump anything stored before
	{Buffer, Arrived, Total} = case SizeOrCont of
		0 -> {State#state.chunkBuf, State#state.chunkSize, State#state.chunkTotal};
		_ ->
			PrevArrive = State#state.chunkSize,
			PrevTotal  = State#state.chunkTotal,
			case PrevArrive of
				0 -> ok;
				_ ->
					% A large transfer timed out, new started, grant the partials allowance
					iris_logger:warn(State#state.logger, "incomplete message discarded",
						[{size, PrevTotal}, {arrived, PrevArrive}]
					),
					iris_conn:tunnel_allowance(State#state.conn, State#state.id, PrevArrive)
			end,
			{[], 0, SizeOrCont}
	end,

	% Append the new chunk and check completion
	NewBuffer  = [Payload | Buffer],
	NewArrived = Arrived + byte_size(Payload),
	case NewArrived of
		Total ->
			Message = binary:list_to_bin(lists:reverse(NewBuffer)),
			{noreply, potentially_recv(State#state{itoaBuf = Queue ++ [Message], chunkBuf = [], chunkSize = 0, chunkTotal = 0})};
		_ ->
			{noreply, State#state{chunkBuf = NewBuffer, chunkSize = NewArrived, chunkTotal = Total}}
	end;

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

  % Notify any pending receive of the closure
  case State#state.itoaPend of
  	nil                  -> ok;
  	{Receiver, RecvTRef} ->
	    case RecvTRef of
	      nil -> ok;
	      _   -> {ok, cancel} = timer:cancel(RecvTRef)
	    end,
	    gen_server:reply(Receiver, {error, closed})
  end,

  % Notify any pending send of the closure
  case State#state.atoiPend of
  	nil                                 -> ok;
    {_Payload, _Sent, Sender, SendTRef} ->
      case SendTRef of
        nil -> ok;
        _   -> {ok, cancel} = timer:cancel(SendTRef)
      end,
      gen_server:reply(Sender, {error, closed})
  end,

  % Notify the connection to dump the tunnel
  ok = iris_conn:handle_tunnel_close(Conn, Id),

  % Notify the closer (if any) of the termination
  case State#state.closer of
    nil  -> {noreply, State#state{itoaPend = nil, term = true, stat = Status}};
    From ->
      gen_server:reply(From, Status),
      {stop, normal, State#state{itoaPend = nil, term = true, stat = Reason}}
  end.


%% @private
%% Notifies the pending send of failure due to timeout. In the rare case of the
%% timeout happening right before the timer is canceled, the event is dropped.
handle_info(send_timeout, State = #state{atoiPend = Task}) ->
	case Task of
		nil                            -> ok;
		{_Payload, _Sent, From, _TRef} -> gen_server:reply(From, {error, timeout})
	end,
	{noreply, State#state{atoiPend = nil}};

%% Notifies the pending recv of failure due to timeout. In the rare case of the
%% timeout happening right before the timer is canceled, the event is dropped.
handle_info(recv_timeout, State = #state{itoaPend = Task}) ->
	case Task of
		nil           -> ok;
		{From, _TRef} -> gen_server:reply(From, {error, timeout})
	end,
	{noreply, State#state{itoaPend = nil}}.

%% @private
%% Cleanup method, does nothing really.
terminate(_Reason, _State) -> ok.


%% =============================================================================
%% Generic server internal methods
%% =============================================================================

%% Sends as many chunks to the relay as data allowance permits.
potentially_send(State = #state{atoiPend = nil}) -> State;

potentially_send(State = #state{atoiPend = Task, atoiSpace = Allowance}) ->
	% Expand the task into it's components
	{Payload, Sent, From, TRef} = Task,
	Size = erlang:min(byte_size(Payload) - Sent, State#state.chunkLimit),
	case Allowance >= Size of
		true ->
			% Calculate the chunk size and fetch the data blob
			SizeOrCont = case Sent of
				0 -> byte_size(Payload);
				_ -> 0
			end,
			Chunk = binary:part(Payload, Sent, Size),

			NewAllowance = Allowance - Size,
			NewSent      = Sent + Size,
			NewTask      = {Payload, NewSent, From, TRef},

			% Send over a data chunk
			ok = iris_conn:tunnel_send(State#state.conn, State#state.id, SizeOrCont, Chunk),

			% Either finish, or enqueue another send
			case NewSent == byte_size(Payload) of
				 true ->
					% Message fully sent, reply to the sender
					case TRef of
						nil -> ok;
						_   -> {ok, cancel} = timer:cancel(TRef)
					end,
					gen_server:reply(From, ok),
					State#state{atoiPend = nil, atoiSpace = NewAllowance};
				false ->
					% Chunks still remaining, potentially send another one
					potentially_send(State#state{atoiPend = NewTask, atoiSpace = NewAllowance})
			end;
		false -> State
	end.


%% Delivers a message to a pending receive.
potentially_recv(State = #state{itoaPend = nil}) -> State;
potentially_recv(State = #state{itoaBuf = []})   -> State;

potentially_recv(State = #state{itoaPend = Wait, itoaBuf = [First | Rest]}) ->
	{From, TRef} = Wait,
	case TRef of
		nil    -> ok;
		_Other ->	{ok, cancel} = timer:cancel(TRef)
	end,
	case iris_conn:tunnel_allowance(State#state.conn, State#state.id, byte_size(First)) of
		ok    -> gen_server:reply(From, {ok, First});
		Error -> gen_server:reply(From, Error)
	end,
	State#state{itoaPend = nil, itoaBuf = Rest}.


%% =============================================================================
%% Unused generic server methods
%% =============================================================================

%% @private
code_change(_OldVsn, _State, _Extra) ->
	{error, unimplemented}.
