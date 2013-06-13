%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

%% @private

-module(iris_proto).

-export([version/0]).
-export([sendInit/2, sendBroadcast/3, sendRequest/5, sendReply/3,
	sendSubscribe/2, sendPublish/3, sendUnsubscribe/2, sendClose/1,
	sendTunnelRequest/5, sendTunnelReply/4, sendTunnelData/3, sendTunnelAck/2,
	sendTunnelClose/2, process/3, procInit/1]).

-define(OP_INIT,      16#00). %% Connection initialization
-define(OP_BCAST,     16#01). %% Application broadcast
-define(OP_REQ,       16#02). %% Application request
-define(OP_REP,       16#03). %% Application reply
-define(OP_SUB,       16#04). %% Topic subscription
-define(OP_PUB,       16#05). %% Topic publish
-define(OP_UNSUB,     16#06). %% Topic subscription removal
-define(OP_CLOSE,     16#07). %% Connection closing
-define(OP_TUN_REQ,   16#08). %% Tunnel building request
-define(OP_TUN_REP,   16#09). %% Tunnel building reply
-define(OP_TUN_DATA,  16#0a). %% Tunnel data transfer
-define(OP_TUN_ACK,   16#0b). %% Tunnel data acknowledgement
-define(OP_TUN_CLOSE, 16#0c). %% Tunnel closing

%% Implemented relay protocol version.
-spec version() -> [byte()].

version() -> "v1.0".

%% Serializes a variable int into an iolist.
-spec packVarint(Num :: non_neg_integer()) ->
	Stream :: iolist().

packVarint(Num) when Num < 128 -> Num;
packVarint(Num)                -> [128 + Num rem 128, packVarint(Num div 128)].

%% Serializes a length-tagged binary array into an iolist.
-spec packBinary(Binary :: binary()) ->
	Stream :: iolist().

packBinary(Binary) ->
	[packVarint(byte_size(Binary)), Binary].

%% Serializes a length-tagged string into the relay.
-spec packString(String :: string()) ->
	Stream :: iolist().

packString(String) ->
	packBinary(list_to_binary(String)).

%% Assembles and serializes an init packet into the relay.
-spec sendInit(Socket :: port(), App :: string()) ->
	ok | {error, Reason :: atom()}.

sendInit(Socket, App) ->
	gen_tcp:send(Socket, [?OP_INIT, packString(version()), packString(App)]).

%% Assembles and serializes a broadcast packet into the relay.
-spec sendBroadcast(Socket :: port(), App :: string(), Message :: binary()) ->
	ok | {error, Reason :: atom()}.

sendBroadcast(Socket, App, Message) ->
	gen_tcp:send(Socket, [?OP_BCAST, packString(App), packBinary(Message)]).

%% Assembles and serializes a request packet into the relay.
-spec sendRequest(Socket :: port(), ReqId :: non_neg_integer(), App :: string(),
	Req :: binary(), Timeout :: pos_integer()) ->
	ok | {error, Reason :: atom()}.

sendRequest(Socket, ReqId, App, Req, Timeout) ->
	gen_tcp:send(Socket, [?OP_REQ, packVarint(ReqId), packString(App), packBinary(Req), packVarint(Timeout)]).

%% Assembles and serializes a reply packet into the relay.
-spec sendReply(Socket :: port(), ReqId :: non_neg_integer(), Reply :: binary()) ->
	ok | {error, Reason :: atom()}.

sendReply(Socket, ReqId, Reply) ->
	gen_tcp:send(Socket, [?OP_REP, packVarint(ReqId),	packBinary(Reply)]).

%% Assembles and serializes a subscription packet into the relay.
-spec sendSubscribe(Socket :: port(), Topic :: string()) ->
	ok | {error, Reason :: atom()}.

sendSubscribe(Socket, Topic) ->
	gen_tcp:send(Socket, [?OP_SUB, packString(Topic)]).

%% Assembles and serializes a message to be published in a topic.
-spec sendPublish(Socket :: port(), Topic :: string(), Message :: binary()) ->
	ok | {error, Reason :: atom()}.

sendPublish(Socket, Topic, Message) ->
	gen_tcp:send(Socket, [?OP_PUB, packString(Topic), packBinary(Message)]).

%% Assembles and serializes a subscription removal packet into the relay.
-spec sendUnsubscribe(Socket :: port(), Topic :: string()) ->
	ok | {error, Reason :: atom()}.

sendUnsubscribe(Socket, Topic) ->
	gen_tcp:send(Socket, [?OP_UNSUB, packString(Topic)]).

%% Assembles and serializes a close packet into the relay.
-spec sendClose(Socket :: port()) ->
	ok | {error, Reason :: atom()}.

sendClose(Socket) ->
	gen_tcp:send(Socket, [?OP_CLOSE]).

%% Assembles and serializes a tunneling packet into the relay.
-spec sendTunnelRequest(Socket :: port(), TunId :: non_neg_integer(), App :: string(),
	Buffer :: pos_integer(), Timeout :: pos_integer()) ->
	ok | {error, Reason :: atom()}.

sendTunnelRequest(Socket, TunId, App, Buffer, Timeout) ->
	gen_tcp:send(Socket, [?OP_TUN_REQ, packVarint(TunId), packString(App), packVarint(Buffer), packVarint(Timeout)]).

%% Assembles and serializes a tunneling confirmation packet into the relay.
-spec sendTunnelReply(Socket :: port(), TmpId :: non_neg_integer(),
	TunId :: non_neg_integer(), Buffer :: pos_integer()) ->
	ok | {error, Reason :: atom()}.

sendTunnelReply(Socket, TmpId, TunId, Buffer) ->
	gen_tcp:send(Socket, [?OP_TUN_REP, packVarint(TmpId), packVarint(TunId), packVarint(Buffer)]).

%% Assembles and serializes a tunnel data packet into the relay.
-spec sendTunnelData(Socket :: port(), TunId :: non_neg_integer(), Message :: binary()) ->
	ok | {error, Reason :: atom()}.

sendTunnelData(Socket, TunId, Message) ->
	gen_tcp:send(Socket, [?OP_TUN_DATA, packVarint(TunId), packBinary(Message)]).

%% Assembles and serializes a tunnel data acknowledgement into the relay.
-spec sendTunnelAck(Socket :: port(), TunId :: non_neg_integer()) ->
	ok | {error, Reason :: atom()}.

sendTunnelAck(Socket, TunId) ->
	gen_tcp:send(Socket, [?OP_TUN_ACK, packVarint(TunId)]).

%% Assembles and serializes a tunnel atomination message into the relay.
-spec sendTunnelClose(Socket :: port(), TunId :: non_neg_integer()) ->
	ok | {error, Reason :: atom()}.

sendTunnelClose(Socket, TunId) ->
	gen_tcp:send(Socket, [?OP_TUN_CLOSE, packVarint(TunId)]).

%% Retrieves a single byte from the relay.
-spec recvByte(Socket :: port()) ->
	Input :: byte() | {error, Reason :: atom()}.

recvByte(Socket) ->
	case gen_tcp:recv(Socket, 1) of
		{ok, <<Byte:8>>} -> Byte;
		Error            -> Error
	end.

%% Retrieves a boolean from the relay.
-spec recvBool(Socket :: port()) ->
	Input :: boolean() | {error, Reason :: atom()}.

recvBool(Socket) ->
	case recvByte(Socket) of
		Error = {error, _Reason} -> Error;
		0 -> false;
		1 -> true;
		_InvalidBool -> {error, violation}
	end.

%% Retrieves a variable int from the relay.
-spec recvVarint(Socket :: port()) ->
	Input :: non_neg_integer() | {error, Reason :: atom()}.

recvVarint(Socket) ->
	case recvByte(Socket) of
		Error = {error, _Reason} -> Error;
		Byte when Byte < 128     -> Byte;
		Byte ->
			case recvVarint(Socket) of
				Error = {error, _Reason} -> Error;
				Num                      -> Byte - 128 + 128 * Num
			end
	end.

%% Retrieves a length-tagged binary array from the relay.
-spec recvBinary(Socket :: port()) ->
	Input :: binary() | {error, Reason :: atom()}.

recvBinary(Socket) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		Size ->
			case gen_tcp:recv(Socket, Size) of
				{ok, Binary} -> Binary;
				Error        -> Error
			end
	end.

%% Retrieves a length-tagged binary array from the relay.
-spec recvString(Socket :: port()) ->
	Input :: [byte()] | {error, Reason :: atom()}.

recvString(Socket) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		Size ->
			case gen_tcp:recv(Socket, Size) of
				{ok, Binary} -> erlang:binary_to_list(Binary);
				Error        -> Error
			end
	end.

%% Retrieves a connection initialization response and returns whether ok.
-spec procInit(Socket :: port()) ->
	ok | {error, Reason :: atom()}.

procInit(Socket) ->
	case recvByte(Socket) of
		?OP_INIT                 -> ok;
		Error = {error, _Reason} -> Error;
		_InvalidOpcode           -> {error, violation}
	end.

%% Retrieves a remote broadcast message from the relay and notifies the handler.
-spec procBroadcast(Socket :: port(), Handler :: pid()) ->
	ok | {error, Reason :: atom()}.

procBroadcast(Socket, Handler) ->
	case recvBinary(Socket) of
		Error = {error, _Reason} -> Error;
		Msg ->
			Handler ! {broadcast, Msg},
			ok
	end.

%% Retrieves a remote request from the relay.
-spec procRequest(Socket :: port(), Owner :: pid(), Handler :: pid()) ->
	ok | {error, Reason :: atom()}.

procRequest(Socket, Owner, Handler) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		ReqId ->
			case recvBinary(Socket) of
				Error = {error, _Reason} -> Error;
				Req ->
					Handler ! {request, {Owner, ReqId}, Req},
					ok
			end
	end.

%% Retrieves a remote reply to a local request from the relay.
-spec procReply(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

procReply(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		ReqId ->
			case recvBool(Socket) of
				Error = {error, _Reason} -> Error;
				true ->
					Owner ! {reply, ReqId, {error, timeout}},
					ok;
				false ->
					case recvBinary(Socket) of
						Error = {error, _Reason} -> Error;
						Reply ->
							Owner ! {reply, ReqId, {ok, Reply}},
							ok
					end
			end
	end.

%% Retrieves a topic event from the relay.
-spec procPublish(Socket :: port(), Handler :: pid()) ->
	ok | {error, Reason :: atom()}.

procPublish(Socket, Handler) ->
	case recvString(Socket) of
		Error = {error, _Reason} -> Error;
		Topic ->
			case recvBinary(Socket) of
				Error = {error, _Reason} -> Error;
				Msg ->
					Handler ! {publish, Topic, Msg},
					ok
			end
	end.

%% Retrieves a remote tunneling request from the relay.
-spec procTunnelRequest(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

procTunnelRequest(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TmpId ->
			case recvVarint(Socket) of
				Error = {error, _Reason} -> Error;
				Buffer ->
					Owner ! {tunnel_request, TmpId, Buffer},
					ok
			end
	end.

%% Retrieves the remote reply to a local tunneling request from the relay.
-spec procTunnelReply(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

procTunnelReply(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			case recvBool(Socket) of
				Error = {error, _Reason} -> Error;
				true ->
					Owner ! {tunnel_reply, TunId, {error, timeout}},
					ok;
				false ->
					case recvVarint(Socket) of
						Error = {error, _Reason} -> Error;
						Buffer ->
							Owner ! {tunnel_reply, TunId, {ok, Buffer}},
							ok
					end
			end
	end.

%% Retrieves a remote tunnel data message.
-spec procTunnelData(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

procTunnelData(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			case recvBinary(Socket) of
				Error = {error, _Reason} -> Error;
				Msg ->
					Owner ! {tunnel_data, TunId, Msg},
					ok
			end
	end.

%% Retrieves a remote tunnel message acknowledgement.
-spec procTunnelAck(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

procTunnelAck(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			Owner ! {tunnel_ack, TunId},
			ok
	end.

%% Retrieves the remote closure of a tunnel.
-spec procTunnelClose(Socket :: port(), Owner :: pid()) ->
	ok | {error, Reason :: atom()}.

procTunnelClose(Socket, Owner) ->
	case recvVarint(Socket) of
		Error = {error, _Reason} -> Error;
		TunId ->
			Owner ! {tunnel_close, TunId},
			ok
	end.

%% Retrieves messages from the client connection and keeps processing them until
%% either the relay closes (graceful close) or the connection drops.
-spec process(Socket :: port(), Owner :: pid(), Handler :: pid()) ->
	no_return() | {error, Reason :: atom()}.

process(Socket, Owner, Handler) ->
	Res = case recvByte(Socket) of
		Error = {error, _} -> Error;
		?OP_BCAST          -> procBroadcast(Socket, Handler);
		?OP_REQ            -> procRequest(Socket, Owner, Handler);
		?OP_REP            -> procReply(Socket, Owner);
		?OP_PUB            -> procPublish(Socket, Handler);
		?OP_TUN_REQ        -> procTunnelRequest(Socket, Owner);
		?OP_TUN_REP        -> procTunnelReply(Socket, Owner);
		?OP_TUN_DATA       -> procTunnelData(Socket, Owner);
		?OP_TUN_ACK        -> procTunnelAck(Socket, Owner);
		?OP_TUN_CLOSE      -> procTunnelClose(Socket, Owner);
		?OP_CLOSE          -> exit(ok);
		_InvalidOpcode     -> {error, violation}
	end,
	case Res of
		ok              -> process(Socket, Owner, Handler);
		{error, Reason} -> exit(Reason)
	end.
