%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% Contains the wire protocol for communicating with the Iris relay endpoint.

%% The specification version implemented is v1.0-draft2, available at:
%% http://iris.karalabe.com/specs/relay-protocol-v1.0-draft2.pdf

%% @private

-module(iris_proto).

-export([send_init/2, send_broadcast/3, send_request/5, send_reply/4,
	send_subscribe/2, send_publish/3, send_unsubscribe/2, send_close/1,
	send_tunnel_init/4, send_tunnel_confirm/3, send_tunnel_allowance/3,
	send_tunnel_transfer/4, send_tunnel_close/2, proc_init/1]).
-export([start_link/3, process/4]).

% Packet opcodes
-define(OP_INIT,         16#00). % Out: connection initiation            | In: connection acceptance
-define(OP_DENY,         16#01). % Out: <never sent>                     | In: connection refusal
-define(OP_CLOSE,        16#02). % Out: connection tear-down initiation  | In: connection tear-down notification

-define(OP_BROADCAST,    16#03). % Out: application broadcast initiation | In: application broadcast delivery
-define(OP_REQUEST,      16#04). % Out: application request initiation   | In: application request delivery
-define(OP_REPLY,        16#05). % Out: application reply initiation     | In: application reply delivery

-define(OP_SUBSCRIBE,    16#06). % Out: topic subscription               | In: <never received>
-define(OP_UNSUBSCRIBE,  16#07). % Out: topic subscription removal       | In: <never received>
-define(OP_PUBLISH,      16#08). % Out: topic event publish              | In: topic event delivery

-define(OP_TUN_INIT,     16#09). % Out: tunnel construction request      | In: tunnel initiation
-define(OP_TUN_CONFIRM,  16#0a). % Out: tunnel confirmation              | In: tunnel construction result
-define(OP_TUN_ALLOW,    16#0b). % Out: tunnel transfer allowance        | In: <same as out>
-define(OP_TUN_TRANSFER, 16#0c). % Out: tunnel data exchange             | In: <same as out>
-define(OP_TUN_CLOSE,    16#0d). % Out: tunnel termination request       | In: tunnel termination notification

% Protocol constants
-define(PROTO_VERSION, "v1.0-draft2").
-define(CLIENT_MAGIC,  "iris-client-magic").
-define(RELAY_MAGIC,   "iris-relay-magic").

%% Serializes a boolean into an iolist.
-spec pack_boolean(Flag :: boolean()) -> Stream :: iolist().

pack_boolean(false) -> [0];
pack_boolean(true)  -> [1].

%% Serializes a variable int using base 128 encoding into an iolist.
-spec pack_varint(Num :: non_neg_integer()) -> Stream :: iolist().

pack_varint(Num) when Num < 128 -> Num;
pack_varint(Num)                -> [128 + Num rem 128, pack_varint(Num div 128)].

%% Serializes a length-tagged binary array into an iolist.
-spec pack_binary(Binary :: binary()) -> Stream :: iolist().

pack_binary(Binary) ->
	[pack_varint(byte_size(Binary)), Binary].

%% Serializes a length-tagged string into an iolist.
-spec pack_string(String :: [byte()]) -> Stream :: iolist().

pack_string(String) ->
	pack_binary(list_to_binary(String)).

%% Sends a connection initiation.
-spec send_init(Socket :: port(), Cluster :: [byte()]) ->
	ok | {error, Reason :: atom()}.

send_init(Socket, Cluster) ->
	gen_tcp:send(Socket, [?OP_INIT, pack_string(?CLIENT_MAGIC), pack_string(?PROTO_VERSION), pack_string(Cluster)]).

%% Sends a connection tear-down initiation.
-spec send_close(Socket :: port()) ->	ok.

send_close(Socket) ->
	ok = gen_tcp:send(Socket, [?OP_CLOSE]).

%% Sends an application broadcast initiation.
-spec send_broadcast(Socket :: port(), Cluster :: [byte()], Message :: binary()) ->	ok.

send_broadcast(Socket, Cluster, Message) ->
	ok = gen_tcp:send(Socket, [?OP_BROADCAST, pack_string(Cluster), pack_binary(Message)]).

%% Sends an application request initiation.
-spec send_request(Socket :: port(), Id :: non_neg_integer(), Cluster :: [byte()],
	Reqquest :: binary(), Timeout :: pos_integer()) -> ok.

send_request(Socket, Id, Cluster, Request, Timeout) ->
	ok = gen_tcp:send(Socket, [?OP_REQUEST, pack_varint(Id), pack_string(Cluster), pack_binary(Request), pack_varint(Timeout)]).

%% Sends an application reply initiation.
-spec send_reply(Socket :: port(), Id :: non_neg_integer(),
	Reply :: binary(), Fault :: [byte()]) -> ok.

send_reply(Socket, Id, Reply, []) ->
	ok = gen_tcp:send(Socket, [?OP_REPLY, pack_varint(Id), pack_boolean(true), pack_binary(Reply)]);

send_reply(Socket, Id, _Reply, Fault) ->
	ok = gen_tcp:send(Socket, [?OP_REPLY, pack_varint(Id),	pack_boolean(false), pack_string(Fault)]).

%% Sends a topic subscription.
-spec send_subscribe(Socket :: port(), Topic :: [byte()]) -> ok.

send_subscribe(Socket, Topic) ->
	ok = gen_tcp:send(Socket, [?OP_SUBSCRIBE, pack_string(Topic)]).

%% Sends a topic subscription removal.
-spec send_unsubscribe(Socket :: port(), Topic :: [byte()]) -> ok.

send_unsubscribe(Socket, Topic) ->
	ok = gen_tcp:send(Socket, [?OP_UNSUBSCRIBE, pack_string(Topic)]).

%% Sends a topic event publish.
-spec send_publish(Socket :: port(), Topic :: [byte()], Event :: binary()) ->	ok.

send_publish(Socket, Topic, Event) ->
	ok = gen_tcp:send(Socket, [?OP_PUBLISH, pack_string(Topic), pack_binary(Event)]).

%% Sends a tunnel construction request.
-spec send_tunnel_init(Socket :: port(), Id :: non_neg_integer(),
	Cluster :: [byte()], Timeout :: pos_integer()) -> ok.

send_tunnel_init(Socket, Id, Cluster, Timeout) ->
	ok = gen_tcp:send(Socket, [?OP_TUN_INIT, pack_varint(Id), pack_string(Cluster), pack_varint(Timeout)]).

%% Sends a tunnel confirmation.
-spec send_tunnel_confirm(Socket :: port(), BuildId :: non_neg_integer(),
	TunId :: non_neg_integer()) -> ok.

send_tunnel_confirm(Socket, BuildId, TunId) ->
	ok = gen_tcp:send(Socket, [?OP_TUN_CONFIRM, pack_varint(BuildId), pack_varint(TunId)]).

%% Sends a tunnel transfer allowance.
-spec send_tunnel_allowance(Socket :: port(), Id :: non_neg_integer(),
	Space :: non_neg_integer()) -> ok.

send_tunnel_allowance(Socket, Id, Space) ->
	ok = gen_tcp:send(Socket, [?OP_TUN_ALLOW, pack_varint(Id), pack_varint(Space)]).

%% Sends a tunnel data exchange.
-spec send_tunnel_transfer(Socket :: port(), Id :: non_neg_integer(),
	SizeOrCont :: non_neg_integer(), Payload :: binary()) -> ok.

send_tunnel_transfer(Socket, Id, SizeOrCont, Payload) ->
	ok = gen_tcp:send(Socket, [?OP_TUN_TRANSFER, pack_varint(Id), pack_varint(SizeOrCont), pack_binary(Payload)]).

%% Sends a tunnel termination request.
-spec send_tunnel_close(Socket :: port(), Id :: non_neg_integer()) ->	ok.

send_tunnel_close(Socket, Id) ->
	ok = gen_tcp:send(Socket, [?OP_TUN_CLOSE, pack_varint(Id)]).

%% Retrieves a single byte from the relay connection.
-spec recv_byte(Socket :: port()) ->
	{ok, Input :: byte()} | {error, Reason :: atom()}.

recv_byte(Socket) ->
	case gen_tcp:recv(Socket, 1) of
		{ok, <<Byte:8>>} -> {ok, Byte};
		Error            -> Error
	end.

%% Retrieves a boolean from the relay connection.
-spec recv_bool(Socket :: port()) ->
	{ok, Input :: boolean()} | {error, Reason :: atom()}.

recv_bool(Socket) ->
	case recv_byte(Socket) of
		{ok, 0}            -> {ok, false};
		{ok, 1}            -> {ok, true};
		{ok, _InvalidBool} -> {error, invalid_boolean};
    Error              -> Error
	end.

%% Retrieves a variable int in base 128 encoding from the relay connection.
-spec recv_varint(Socket :: port()) ->
	{ok, Input :: non_neg_integer()} | {error, Reason :: atom()}.

recv_varint(Socket) ->
	case recv_byte(Socket) of
		{ok, Byte} when Byte < 128 -> {ok, Byte};
		{ok, Byte} ->
			case recv_varint(Socket) of
				{ok, Num} -> {ok, Byte - 128 + 128 * Num};
				Error     -> Error
			end;
		Error -> Error
	end.

%% Retrieves a length-tagged binary array from the relay connection.
-spec recv_binary(Socket :: port()) ->
	{ok, Input :: binary()} | {error, Reason :: atom()}.

recv_binary(Socket) ->
	case recv_varint(Socket) of
		{ok, Size} -> gen_tcp:recv(Socket, Size);
		Error      -> Error
	end.

%% Retrieves a length-tagged string from the relay connection.
-spec recv_string(Socket :: port()) ->
	{ok, Input :: [byte()]} | {error, Reason :: atom()}.

recv_string(Socket) ->
	case recv_binary(Socket) of
		{ok, Binary} -> {ok, erlang:binary_to_list(Binary)};
		Error        -> Error
	end.

%% Retrieves a connection initiation response (either accept or deny).
-spec proc_init(Socket :: port()) ->
	{ok, Version :: [byte()]} | {error, Reason :: term()}.

proc_init(Socket) ->
  % Retrieve the response opcode
	case recv_byte(Socket) of
    {ok, OpCode} when OpCode =:= ?OP_INIT; OpCode =:= ?OP_DENY ->
      % Verify the opcode validity and relay magic string
      case recv_string(Socket) of
        {ok, ?RELAY_MAGIC} ->
          % Depending on success or failure, proceed and return
          case OpCode of
            ?OP_INIT -> recv_string(Socket);
            ?OP_DENY ->
              % Read the reason for connection denial
              case recv_string(Socket) of
                {ok, Reason} -> {error, io_lib:format("connection denied: ~s", [Reason])};
                Error        -> Error
              end
          end;
        {ok, InvalidMagic} -> {error, io_lib:format("protocol violation: invalid relay magic: ~s", [InvalidMagic])};
        Error -> Error
      end;
		{ok, InvalidOpCode} -> {error, io_lib:format("protocol violation: invalid init response opcode: ~s", [InvalidOpCode])};
		Error -> Error
	end.

%% Retrieves a connection tear-down notification.
-spec proc_close(Socket :: port()) ->	{ok, Reason :: [byte()]}.

proc_close(Socket) ->
	{ok, Reason} = recv_string(Socket),
	{ok, Reason}.

%% Retrieves an application broadcast delivery.
-spec proc_broadcast(Socket :: port(), Handler :: pid()) -> ok.

proc_broadcast(Socket, Handler) ->
	{ok, Message} = recv_binary(Socket),
  ok = iris_conn:handle_broadcast(Handler, Message).

%% Retrieves an application request delivery.
-spec proc_request(Socket :: port(), Owner :: pid(), Handler :: pid()) -> ok.

proc_request(Socket, Owner, Handler) ->
	{ok, Id}      = recv_varint(Socket),
	{ok, Request} = recv_binary(Socket),
	{ok, Timeout} = recv_varint(Socket),
  ok = iris_conn:handle_request(Handler, Owner, Id, Request, Timeout).

%% Retrieves an application reply delivery.
-spec proc_reply(Socket :: port(), Owner :: pid()) -> ok.

proc_reply(Socket, Owner) ->
	{ok, Id}      = recv_varint(Socket),
	{ok, Timeout} = recv_bool(Socket),
	case Timeout of
		true  -> Owner ! {reply, Id, {error, timeout}};
		false ->
			{ok, Success} = recv_bool(Socket),
			case Success of
				true ->
					{ok, Reply} = recv_binary(Socket),
					Owner ! {reply, Id, {ok, Reply}};
				false ->
					{ok, Fault} = recv_string(Socket),
					Owner ! {reply, Id, {error, Fault}}
			end
	end,
	ok.

%% Retrieves a topic event delivery.
-spec proc_publish(Socket :: port(), Owner :: pid()) ->	ok.

proc_publish(Socket, Owner) ->
	{ok, Topic} = recv_string(Socket),
	{ok, Event} = recv_binary(Socket),
	Owner ! {publish, Topic, Event},
	ok.

%% Retrieves a tunnel initiation message.
-spec proc_tunnel_init(Socket :: port(), Owner :: pid()) -> ok.

proc_tunnel_init(Socket, Owner) ->
	{ok, Id}         = recv_varint(Socket),
	{ok, ChunkLimit} = recv_varint(Socket),
	Owner ! {tunnel_init, Id, ChunkLimit},
	ok.

%% Retrieves a tunnel construction result.
-spec proc_tunnel_result(Socket :: port(), Owner :: pid()) -> ok.

proc_tunnel_result(Socket, Owner) ->
	{ok, Id}      = recv_varint(Socket),
	{ok, Timeout} = recv_bool(Socket),
	case Timeout of
		true -> Owner ! {tunnel_result, Id, {error, timeout}};
		false ->
			{ok, ChunkLimit} = recv_varint(Socket),
			Owner ! {tunnel_result, Id, {ok, ChunkLimit}}
	end,
	ok.

%% Retrieves a tunnel transfer allowance message.
-spec proc_tunnel_allowance(Socket :: port(), Owner :: pid()) -> ok.

proc_tunnel_allowance(Socket, Owner) ->
	{ok, Id}    = recv_varint(Socket),
	{ok, Space} = recv_varint(Socket),
	Owner ! {tunnel_alowance, Id, Space},
	ok.

%% Retrieves a tunnel data exchange message.
-spec proc_tunnel_transfer(Socket :: port(), Owner :: pid()) -> ok.

proc_tunnel_transfer(Socket, Owner) ->
	{ok, Id}         = recv_varint(Socket),
	{ok, SizeOrCont} = recv_varint(Socket),
	{ok, Payload}    = recv_binary(Socket),
	Owner ! {tunnel_transfer, Id, SizeOrCont, Payload},
	ok.

%% Retrieves a tunnel closure notification.
-spec proc_tunnel_close(Socket :: port(), Owner :: pid()) -> ok.

proc_tunnel_close(Socket, Owner) ->
	{ok, Id}     = recv_varint(Socket),
	{ok, Reason} = recv_string(Socket),
	Owner ! {tunnel_close, Id, Reason},
	ok.

%% Starts a new packet processor, reading from the given socket and dispatching
%% mostly to self(), with the exception of broadcasts and requests which have
%% bounded mailboxes in front of the handler.
-spec start_link(Socket :: port(), Broadcaster :: pid(), Requester :: pid()) -> pid().

start_link(Socket, Broadcaster, Requester) ->
  spawn_link(?MODULE, process, [Socket, self(), Broadcaster, Requester]).

%% Retrieves messages from the client connection and keeps processing them until
%% either the relay closes (graceful close) or the connection drops.
-spec process(Socket :: port(), Owner :: pid(),
  Broadcaster :: pid(), Requester :: pid()) -> no_return().

process(Socket, Owner, Broadcaster, Requester) ->
	ok = case recv_byte(Socket) of
		{ok, ?OP_BROADCAST}    -> proc_broadcast(Socket, Broadcaster);
		{ok, ?OP_REQUEST}      -> proc_request(Socket, Owner, Requester);
		{ok, ?OP_REPLY}        -> proc_reply(Socket, Owner);
		{ok, ?OP_PUBLISH}      -> proc_publish(Socket, Owner);
		{ok, ?OP_TUN_INIT}     -> proc_tunnel_init(Socket, Owner);
		{ok, ?OP_TUN_CONFIRM}  -> proc_tunnel_result(Socket, Owner);
    {ok, ?OP_TUN_ALLOW}    -> proc_tunnel_allowance(Socket, Owner);
		{ok, ?OP_TUN_TRANSFER} -> proc_tunnel_transfer(Socket, Owner);
		{ok, ?OP_TUN_CLOSE}    -> proc_tunnel_close(Socket, Owner);
		{ok, ?OP_CLOSE} ->
			{ok, ""} = proc_close(Socket),
			exit(ok)
	end,
	process(Socket, Owner, Broadcaster, Requester).
