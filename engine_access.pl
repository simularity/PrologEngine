% engine_access.pl - Access to the HPCE from Prolog
% Copyright 2015, Simularity, Inc

:- module(engine_access, [get_segments/2, get_item_size/3, engine_get_type/3,
			  engine_get_object/4, engine_get_action/3, engine_fold/8,
			  engine_types/2, engine_save/1, engine_save/2,
			  engine_load/1, engine_load/2]).

:- use_module(library(http/http_header)).
:- use_module(library(http/http_client)).
:- use_module(library(http/json)).


:- use_module(library(http/http_header)).
:- use_module(library(http/http_client)).
:- use_module(library(http/json)).


get_segments(Host:Port, Segments) :-
	format(atom(URL), 'http://~w:~w/segments', [Host, Port]),
	% This one doesn't have keep alive
	http_get(URL, JSON, []),
	atom_json_term(JSON, JTerm, []),
	JTerm = json(JSONList),
	member(segments=JList, JSONList),
	maplist(to_segment, JList, Segments), !.

get_segments(Host:Port, _) :-
	throw(error(engine_access(Host:Port), get_segments/2)).




get_item_size(Host:Port, Max, Min) :-
	format(atom(URL), 'http://~w:~w/itemsize', [Host, Port]),
	% This one doesn't have keep alive
	http_get(URL, JSON, []),
	atom_json_term(JSON, JTerm, []),
	JTerm = json(JSONList),
	member(max=Max, JSONList),
	member(min=Min, JSONList),
	!.

to_segment(json(List), segment(Number, Host:Port)) :-
	member(segment=Number, List),
	member(host=Host, List),
	member(sport=Port, List).

engine_get_type(_Host:_Port, Name, Type) :-
	Name =.. [Class, Value],
	Type =.. [Class, Hash],
	integer(Value),
	Hash = Value, !.

engine_get_type(Host:Port, Name, Type) :-
	Name =.. [Class, Value],
	Type =.. [Class, Hash],

	format(atom(URL), 'http://~w:~w/convert_type?class=~w&name=~w',
	       [Host, Port, Class, Value]),
	http_get(URL, JDoc, []),
	((atom_json_term(JDoc, JSON, []),
	  JSON=json(JList), member(status=0, JList),
	  member(id=Hash, JList)) ->
	 assertz(type_map(Name, Type))
	;
	 throw(error(web_transaction(Host:Port, URL), get_type/3))).

engine_get_object(_, ID, _Type, ID) :-
	integer(ID), !.

engine_get_object(Host:Port, ObjName, Type, Code) :-
	engine_get_type(Host:Port, Type, TypeSpec),
	TypeSpec =.. [Class, TypeId],

	uri_encoded(query_value, ObjName, Encoded), 
	format(atom(URL), 'http://~w:~w/convert_object?name=~w&class=~w&type=~w', [Host, Port, Encoded, Class, TypeId]),

	http_get(URL, JDoc, []),

	((atom_json_term(JDoc, Json, []),
	  Json=json(JList), member(status=0, JList),
	  member(id=Code, JList)) ->
	     asserta(object_def(ObjName, TypeSpec, Code))
	;
	 throw(error(web_transaction(Host:Port, URL), get_object/4))).

engine_get_action(_, Action, Action) :-
	integer(Action), !.
engine_get_action(Host:Port, Action, Code) :-
	format(atom(URL), 'http://~w:~w/convert_action?name=~w',
	       [Host, Port, Action]),

	http_get(URL, JDoc, []),
	 
	((atom_json_term(JDoc, JSon, []), JSon = json(JList),
	  member(status=0, JList), member(id=Code, JList)) ->
	 asserta(action_def(Action, Code))
	;
	 throw(error(web_transaction(Host:Port, URL), get_action/3))).



map_action(forward(Val), faction=Val).
map_action(reverse(Val), raction=Val).
map_action(Val, action=Val) :- atomic(Val).

map_type(subject(S), stype=S).
map_type(object(O), otype=O).

corr_to_term(json(JList), correlation(Object, Value, I, A2)) :-
	member(type=Type, JList),
	member(item=Item, JList),
	member(value=Value, JList),
	member(i=I, JList),
	member(a2=A2, JList),
	(member(class=Class, JList) ->
	 true
	;
	 Class = object),
	(member(obj_name=Name, JList) ->
	 ObjValue = Item-Name
	;
	 ObjValue = Item),
	Object =.. [Class, Type, ObjValue], !.

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

engine_types(Host:Port, TypesList) :-
	format(atom(URL), 'http://~w:~w/types', [Host, Port]),
	http_get(URL, JDoc, []),
	atom_json_term(JDoc, JSon, []),
	JSon = json(JList),
	member(status=Status, JList),
	(Status =:= 0 ->
	member(types=TypesJSON, JList),
	maplist(json_type, TypesJSON, TypesList)
	;
	member(status=Status, JList),
	throw(status(Status))), !.

json_type(json(List), Type) :-
	member(class=Class, List),
	member(name=Name, List),
	member(value=TypeID, List),
	findall(Attr, member(attribute=Attr, List), Attrs),
	Type =.. [Class, TypeID, Name, Attrs].

engine_save(Host:Port) :-
	format(atom(URL), 'http://~w:~w/save', [Host, Port]),
		http_get(URL, JDoc, []),
	atom_json_term(JDoc, JSon, []),
	JSon = json(JList),
	member(status=Status, JList),
	(Status =:= 0 ->
	 true
	;
	 throw(status(Status))).

engine_save(Host:Port, Name) :-
	format(atom(URL), 'http://~w:~w/save?name=~w', [Host, Port, Name]),
		http_get(URL, JDoc, []),
	atom_json_term(JDoc, JSon, []),
	JSon = json(JList),
	member(status=Status, JList),
	(Status =:= 0 ->
	 true
	;
	 throw(status(Status))).

engine_load(Host:Port) :-
	format(atom(URL), 'http://~w:~w/restore', [Host, Port]),
		http_get(URL, JDoc, []),
	atom_json_term(JDoc, JSon, []),
	JSon = json(JList),
	member(status=Status, JList),
	(Status =:= 0 ->
	 true
	;
	 throw(status(Status))).

engine_load(Host:Port, Name) :-
	format(atom(URL), 'http://~w:~w/restore?name=~w', [Host, Port, Name]),
		http_get(URL, JDoc, []),
	atom_json_term(JDoc, JSon, []),
	JSon = json(JList),
	member(status=Status, JList),
	(Status =:= 0 ->
	 true
	;
	 throw(status(Status))).
