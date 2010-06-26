simplevt.sqlext: simplevt.c
	gcc -g -bundle -fPIC -Isqlite3 -o $@ $<

clean: 
	$(RM) simplevt.sqlext

