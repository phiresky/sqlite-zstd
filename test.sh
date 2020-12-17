set -e

sqlite3 foo 'create table tbl(cola text primary key, colb int);'

sqlite3 foo 'create view vw as select * from tbl;'

sqlite3 foo 'insert into tbl values ("cola1", 1);'
sqlite3 foo 'insert into tbl values ("cola2", 2);'
sqlite3 foo 'insert into tbl values ("cola3", 3);'


sqlite3 foo 'create table log(ins integer primary key, info txt);'
#sqlite3 foo 'create trigger t1 instead of update of cola on vw for each row begin update tbl set cola = new.cola where cola = old.cola; end;'
#sqlite3 foo 'create trigger t2 instead of update of colb on vw for each row begin update tbl set colb = new.colb where cola = old.cola; end;'


# sqlite3 foo 'create trigger t2 instead of update on vw for each row begin update tbl set cola = new.cola, colb = new.colb where cola = old.cola; end;'

sqlite3 foo 'create trigger t2 instead of update of cola on vw for each row begin insert into log (info) values (new.cola); end;'
sqlite3 foo 'create trigger t3 instead of update of colb on vw for each row begin insert into log (info) values (new.colb); end;'