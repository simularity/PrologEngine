% engine.pl - prolog interface to the HPCE
% Copyright 2015, Simularity, Inc

:- module(engine, [
		   engine_load/3,
		   engine_get_type/3,
		   engine_get_object/4,
		   engine_save/1,
		   engine_restore/1,
		   engine_fold/8,
		   engine_hash/2]).

:- use_module(library(http/http_header)).
:- use_module(library(http/http_client)).
:- use_module(library(http/json)).
:- use_module(library(socket)).

%:- dynamic segment/6.
:- thread_local type_def/2.
:- thread_local object_def/3.
:- thread_local action_def/2.
:- thread_local segment/3.
:- thread_local segment_segs/1.
:- thread_local seg_post/1.  % Mark that we can use 'Keep-Alive' on conn

%:- dynamic connection/5.

:- thread_local action/1.

:- multifile apply_action/6.

:- meta_predicate engine_load(+,+,1).

engine_hash(Atom, Hash) :-
	atom_codes(Atom, String),
	engine_hash(String, 5381, Hash).

engine_hash([], Hash, Hash).
engine_hash([V | T], Val, Hash) :-
	Next is (((Val << 5) + Val) + V) mod 0x7fffffffffffffff,
	engine_hash(T, Next, Hash).


retract_pred(Pred) :-
	Closure =.. [Pred, _],
	retractall(Closure).

clear_local_preds :- 
	findall(Pred, segment(_, _, Pred), Preds),
	maplist(retract_pred, Preds),
	retractall(type_def(_, _)),
	retractall(object_def(_, _, _)),
	retractall(action_def(_, _)),
	retractall(segment(_, _, _)),
	retractall(segment_segs(_)),
	retractall(seg_post(_)).


% engine_connect sets thread_local predicates that guide the loading
engine_connect(Host:Port) :-

	format(atom(URL), 'http://~w:~w/segments', [Host, Port]),
	% This one doesn't have keep alive
	http_get(URL, JSON, []),
	atom_json_term(JSON, JTerm, []),
	JTerm = json(JSONList),
	member(segments=JList, JSONList),
	maplist(set_segment, JList),
	length(JList, NSegs),
	asserta(segment_segs(NSegs)).

% set_segment sets the tl predicate segment/4 which allows the 
set_segment(json(List)) :-
	format('~w~n', [List]),
	member(segment=Seg, List),
	member(host=Host, List),
	member(sport=Sport, List),
	atomic_list_concat([set_segment_, Seg], SegPred),
	thread_local(SegPred/1),
	
% assert the segment. Remember that this is thread local
	assertz(segment(Seg, Host:Sport, SegPred)).


engine_load(HostSpec, Volume, Closure) :-
	clear_local_preds,
	engine_connect(HostSpec),
	% Start with a clean TL Database
	call(Closure, Term),
	apply(HostSpec, Volume, Term),
	fail.

engine_load(_Host:_Spec, _Volume, _Closure) :-
% First, we flush our transactions
	segment_segs(NSegs),
	TopSeg is NSegs - 1,
	transact_all(TopSeg),
	% To finish, retract all our stuff
	clear_local_preds.

transact_all(Seg) :-
	(Seg < 0 ->
	 true
	;
	 transact(Seg),
	 Next is Seg - 1,
	 transact_all(Next)).


transact(Seg) :-
	catch(transact_(Seg), E, (!, format('Exception ~w~n', [E]),
				  transact(Seg))).

transact_(Seg) :-
	segment(Seg, Host:Port, SegPred),
	Closure =.. [SegPred, Term],
	findall(Term, Closure, Output),

	
	% Alter the list to Codes for use with http_post
	write_term_to_codes(Output, Codes, []),
	!,
	format(atom(URL), 'http://~w:~w/load_data', [Host, Port]),
	(seg_post(Seg) ->
	 Parms = ['Keep-Alive']
	;
	 
	 Parms = [],
	 assertz(seg_post(Seg))),
	
	http_post(URL, codes(Codes), _Result, Parms),
	retractall(Closure).

get_action(Host:Port, Action, Code) :-
	(action_def(Action, Code) ->
	 true
	;
	 format(atom(URL), 'http://~w:~w/convert_action?name=~w',
		[Host, Port, Action]),

	 http_get(URL, JDoc, [connection('Keep-Alive')]),
	 
	 ((atom_json_term(JDoc, JSon, []), JSon = json(JList),
	   member(status=0, JList), member(id=Code, JList)) ->
	  asserta(action_def(Action, Code))
	 ;
	  throw(error(web_transaction(Host:Port, URL), get_action/3)))).


get_object(Host:Port, Type, ObjName, Code) :-
	Type =.. [Class, TypeNum],
	(atom(TypeNum) ->
	 get_type(Host:Port, Type, TypeId)
	;
	 TypeNum = TypeId),
	TypeSpec =.. [Class, TypeId],
	(object_def(ObjName, TypeSpec, Code) ->
	 true
	;
	 uri_encoded(query_value, ObjName, Encoded), 
	 format(atom(URL), 'http://~w:~w/convert_object?name=~w&class=~w&type=~w', [Host, Port, Encoded, Class, TypeId]),

	 http_get(URL, JDoc, [connection('Keep-Alive')]),

	 ((atom_json_term(JDoc, Json, []),
	   Json=json(JList), member(status=0, JList),
	   member(id=Code, JList)) ->

	  asserta(object_def(ObjName, TypeSpec, Code))
	 ;
	  throw(error(web_transaction(Host:Port, URL), get_object/4)))).

engine_get_type(Spec, Type, Code) :-
	get_type(Spec, Type, Code).

engine_get_object(Spec, Type, ObjName, Code) :-
	get_object(Spec, Type, ObjName, Code).

get_type(Host:Port, Type, Code) :-
	Type =.. [Class, TypeId],
	(resolve_type(Type, Code) ->
	 true
	;
	 format(atom(URL), 'http://~w:~w/convert_type?class=~w&name=~w', [Host, Port, Class, TypeId]),
	 http_get(URL, JDoc, [connection('Keep-Alive')]),
	 ((atom_json_term(JDoc, JSON, []),
	   JSON=json(JList), member(status=0, JList),
	   member(id=Code, JList)) ->
	  assertz(type_def(Type, Code))
	 ;
	  throw(error(web_transaction(Host:Port, URL), get_type/3)))).

resolve_type(Type, Code) :-
	Type =.. [_Class, TypeSpec],
	(atom(TypeSpec) ->
	 type_def(Type, Code)
	;
	 Code = TypeSpec).

% Apply an "Add Triple"
apply(Host:Port, Volume, Term) :-
	Term = triple(SType, SObj, Action, OType, OObj),
	!,
	(atom(SType) ->
	 get_type(Host:Port, subject(SType), STypeId)
	;
	 STypeId = SType),

	(atom(OType) ->
	 get_type(Host:Port, object(OType), OTypeId)
	;
	 OTypeId = OType),

	(atom(SObj) ->
	 get_object(Host:Port, subject(STypeId), SObj, SObjId)
	;
	 SObjId = SObj),

	(atom(OObj) ->
	 get_object(Host:Port, object(OTypeId), OObj, OObjId)
	;
	 OObjId = OObj),

	(atom(Action) ->
	 get_action(Host:Port, Action, ActionId)
	;
	 ActionId = Action),
	
	segment_segs(NSegs),

	OSeg is floor(OObjId) mod NSegs,
	SSeg is floor(SObjId) mod NSegs,
	% Pass them down to the accumulator
	accumulate(OSeg, Volume,
		   triple(subject(STypeId, SObjId), ActionId,
			  object(OTypeId, OObjId))),

	accumulate(SSeg, Volume,
		   triple(object(OTypeId, OObjId), ActionId,
			  subject(STypeId, SObjId))).

% Apply a "delete Triple
apply(Host:Port, Volume, Term) :-
	Term = triple(SType, SObj, OType, OObj),
	!,
	(atom(SType) ->
	 get_type(Host:Port, subject(SType), STypeId)
	;
	 STypeId = SType),

	(atom(OType) ->
	 get_type(Host:Port, object(OType), OTypeId)
	;
	 OTypeId = OType),

	(atom(SObj) ->
	 get_object(Host:Port, subject(STypeId), SObj, SObjId)
	;
	 SObjId = SObj),

	(atom(OObj) ->
	 get_object(Host:Port, object(OTypeId), OObj, OObjId)
	;
	 OObjId = OObj),

	segment_segs(NSegs),

	OSeg is floor(OObjId) mod NSegs,
	SSeg is floor(SObjId) mod NSegs,

	% Pass them down to the accumulator
	accumulate(OSeg, Volume,
		   triple(subject(STypeId, SObjId), object(OTypeId, OObjId))),

	accumulate(SSeg, Volume,
		   triple(object(OTypeId, OObjId), subject(STypeId, SObjId))).


accumulate(Seg, Volume, Term) :-
	segment(Seg, _, SegPred),
	Closure =.. [SegPred, Term],
	assertz(Closure),
	Head =.. [SegPred, _],
	predicate_property(Head, number_of_clauses(Next)), 
	(Next >= Volume ->
	 transact(Seg)
	;
	 true).


engine_save(Host:Port) :-
	format(atom(URL), 'http://~w:~w/save', [Host, Port]),
	http_get(URL, Res, []),

	atom_json_term(Res, JTerm, []),
	JTerm = json(JSONList),
	member(status=0, JSONList), !.

engine_restore(Host:Port) :-
	format(atom(URL), 'http://~w:~w/restore', [Host, Port]),
	http_get(URL, Res, []),

	atom_json_term(Res, JTerm, []),
	JTerm = json(JSONList),
	member(status=0, JSONList), !.

map_action(forward(Val), faction=Val).
map_action(reverse(Val), raction=Val).
map_action(Val, action=Val) :- atomic(Val).

map_type(subject(S), stype=S).
map_type(object(O), otype=O).

engine_fold(Host:Port, Expr, Actions, Types, Metric, Legit, Count, Res) :-
	write_term_to_codes(Expr, Codes, []),
	maplist(map_action, Actions, URL_Act),
	maplist(map_type, Types, URL_Type),

	flatten([URL_Act, URL_Type, [metric=Metric, legit=Legit,
				     count=Count, use_legit=true]],
		URL_Terms),
	maplist(term_to_atom, URL_Terms, URL_Atoms),
	atomic_list_concat(URL_Atoms, '&', URL_String),
	format(atom(URL), 'http://~w:~w/expression?~w', [Host, Port,
							 URL_String]),
	format('~w~n', [URL]),
	http_post(URL, codes(Codes), Result, []),
	atom_json_term(Result, JTerm, []),
	JTerm = json(JSONList),
	member(status=Status, JSONList),
	(Status =:= 0 ->
	 member(a1=A1, JSONList),
	 member(global=Global, JSONList),
	 member(correlation=Corr, JSONList),
	 maplist(corr_to_term, Corr, List),!,
	 Res = results(A1, Global, List)
	;
	 throw(status(Status))).

corr_to_term(json(JList), correlation(Type, Item, Value, I, A2)) :-
	member(type=Type, JList),
	member(item=Item, JList),
	member(value=Value, JList),
	member(i=I, JList),
	member(a2=A2, JList), !.

	

test_values(Count, STRange, SORange, OTRange, OORange,
	    triple(St, Si, 1, Ot, Oi)) :-
	between(1, STRange, STv),
	between(1, SORange, SOv),
	between(1, OTRange, OTv),
	between(1, OORange, OOv),
	(Count = count(M) ->
	 N is M + 1,
	 nb_setarg(1, Count, N),
	 (N mod 10000 =:= 0 ->
	  format('Iteration ~w~n', N)
	 ;
	  true)
	;
	 true), 

%	atomic_list_concat([subject_, STv], St),
%	atomic_list_concat([sobj_, SOv], Si),
%	atomic_list_concat([object_, OTv], Ot),
%	atomic_list_concat([oobj_, OOv], Oi).

	St = STv,
	Si = SOv,
	Ot = OTv,
	Oi = OOv.
	