cdef extern from 'stdlib.h':
    void free(void *)

cdef extern from "Python.h":
    void  Py_INCREF(object o)
    void  Py_DECREF(object o)

cdef extern from 'cabin.h':

    # This is a qdbm header
    ctypedef struct CBLIST:
        pass

    void cblistclose(CBLIST *list)                          # close cblist
    int cblistnum(CBLIST *list)                             # length of cblist
    char *cblistval(CBLIST *list, int index, int *sp)       # get value
    void cblistsort(CBLIST *list)                           # obvious

cdef extern from 'estraier.h':

    # Document open mode flags
    cdef enum:
        ESTDBREADER = 1 << 0
        ESTDBWRITER = 1 << 1
        ESTDBCREAT = 1 << 2
        ESTDBTRUNC = 1 << 3
        ESTDBNOLCK = 1 << 4
        ESTDBLCKNB = 1 << 5
        ESTDBPERFNG = 1 << 6

    # Condition flags
    cdef enum:
        ESTCONDSURE = 1 << 0      # check every N-gram key
        ESTCONDUSUAL = 1 << 1     # check N-gram keys skipping by one
        ESTCONDFAST = 1 << 2      # check N-gram keys skipping by two
        ESTCONDAGITO = 1 << 3     # check N-gram keys skipping by three
        ESTCONDNOIDF = 1 << 4     # without TF-IDF tuning
        ESTCONDSIMPLE = 1 << 10   # with the simplified phrase
        ESTCONDSCFB = 1 << 30     # feed back scores (for debug)

    # Document registration modes
    cdef enum:
        ESTPDCLEAN = 1 << 0

    cdef enum:
        ESTGDNOATTR = 1 << 0      # no attributes
        ESTGDNOTEXT = 1 << 1      # no text

    cdef enum:
        ESTOPTNOPURGE = 1 << 0    # omit purging dispensable region of deleted
        ESTOPTNODBOPT = 1 << 1    # omit optimization of the database files

    cdef enum:                    # enumeration for options of document deletion
        ESTODCLEAN = 1 << 0       # clean up dispensable regions

    ctypedef struct ESTDB:
        pass

    ctypedef struct ESTDOC:
        pass

    ctypedef struct ESTCOND:
        pass

    ctypedef struct CBMAP:
        pass

    char *est_err_msg(int ecode)

    # Database API
    ESTDB *est_db_open(char *name, int omode, int *ecp)
    int est_db_close(ESTDB *db, int *ecp)
    int est_db_put_doc(ESTDB *db, ESTDOC *doc, int options)
    int *est_db_search(ESTDB *db, ESTCOND *cond, int *nump, CBMAP *hints)
    ESTDOC *est_db_get_doc(ESTDB *db, int id, int options)
    char *est_db_name(ESTDB *db)
    int est_db_doc_num(ESTDB *db)
    double est_db_size(ESTDB *db)
    int est_db_flush(ESTDB *db, int max)
    int est_db_sync(ESTDB *db)
    int est_db_optimize(ESTDB *db, int options)
    int est_db_uri_to_id(ESTDB *db, char *uri)
    # These are part of the Database API but they really should stay in the
    # Document like: Document.remove() and the other one used under the
    # hood when changing a document.
    int est_db_out_doc(ESTDB *db, int id, int options)
    int est_db_edit_doc(ESTDB *db, ESTDOC *doc)
    int est_db_error(ESTDB *db)
    int est_db_fatal(ESTDB *db)

    # Document API
    ESTDOC *est_doc_new()
    void est_doc_delete(ESTDOC *doc)
    void est_doc_add_attr(ESTDOC *doc, char *name, char *value)
    void est_doc_add_text(ESTDOC *doc, char *text)
    char *est_doc_attr(ESTDOC *doc, char *name)
    void est_doc_add_hidden_text(ESTDOC *doc, char *text)
    int est_doc_id(ESTDOC *doc)
    ESTDOC *est_doc_new_from_draft(char *draft)
    CBLIST *est_doc_attr_names(ESTDOC *doc)

    ## Document API that still needs wrapping before the end.
    CBLIST *est_doc_texts(ESTDOC *doc) # list of the texts added to the document
    char *est_doc_cat_texts(ESTDOC *doc) # this is the same as above but
    # returns the list concatenated. this is hard to manage memory wise and we
    # can build this on top of the other one above
    char *est_doc_dump_draft(ESTDOC *doc) # is this worth?
    # Creates the snippet with highlighted mathing *words in the *doc
    char *est_doc_make_snippet(ESTDOC *doc, CBLIST *words, int wwidth, int hwidth, int awidth)

    # Condition API
    ESTCOND *est_cond_new()
    void est_cond_delete(ESTCOND *cond)
    void est_cond_set_phrase(ESTCOND *cond, char *phrase)
    void est_cond_add_attr(ESTCOND *cond, char *expr)
    void est_cond_set_order(ESTCOND *cond, char *expr)
    void est_cond_set_max(ESTCOND *cond, int max)
    void est_cond_set_options(ESTCOND *cond, int options)
    int est_cond_score(ESTCOND *cond, int index)

class HyperEstraierError(Exception):
    pass

class DBError(HyperEstraierError):
    pass

class DocumentError(HyperEstraierError):
    pass

class DocModifyImmutableError(DocumentError):
    pass

class DocNeverAddedError(DocumentError):
    pass

class DBEditError(DBError):
    pass

class DBRemoveError(DBError):
    pass

class DBFlushError(DBError):
    pass

class DBSyncError(DBError):
    pass

class DBOptimizeError(DBError):
    pass

cdef class Database # Forward

cdef class Document:
    cdef ESTDOC *estdoc
    cdef Database db

    def __dealloc__(self):
        if self.db is not None:
            Py_DECREF(self.db)
        if self.estdoc != NULL:
            est_doc_delete(self.estdoc)
            self.estdoc = NULL

    cdef init_estdoc(self):
        """
        Internal function to ensure estdoc is allocated.
        """
        if self.estdoc == NULL:
            self.estdoc = est_doc_new()

    property id:
        " Document ID "
        def __get__(self):
            self.init_estdoc()
            return est_doc_id(self.estdoc)

    property uri:
        def __get__(self):
            return self.get('@uri')

    property attributes:
        " A list of attribute names "
        def __get__(self):
            cdef CBLIST *attrs_c
            cdef int attrs_length, i, sp
            self.init_estdoc()
            attrs_c = est_doc_attr_names(self.estdoc)
            attrs_length = cblistnum(attrs_c)
            cblistsort(attrs_c)
            attrs = []
            for i from 0 <= i < attrs_length:
                attrs.append(cblistval(attrs_c, i, &sp))
            cblistclose(attrs_c)
            return attrs

    def __getitem__(self, name):
        value = self.get(name)
        if value is not None:
            return value
        raise KeyError('Document has no attribute %r'%name)

    def __setitem__(self, name, value):
        self.init_estdoc()
        if name == "@uri" and self.get('@uri', None):
            raise DocModifyImmutableError("Cannot modify @uri attribute")
        est_doc_add_attr(self.estdoc, name, value)

    def get(self, name, default=None):
        cdef char *value
        self.init_estdoc()
        value = est_doc_attr(self.estdoc, name)
        if value == NULL:
            return default
        return value

    def add_text(self, text):
        self.init_estdoc()
        est_doc_add_text(self.estdoc, text)

    def add_hidden_text(self, text):
        self.init_estdoc()
        est_doc_add_hidden_text(self.estdoc, text)

    def commit(self):
        if self.db is not None:
            return self.db.commit(self)
        raise DocNeverAddedError("Cannot commit an object that was never added")

    def remove(self, int options = ESTODCLEAN):
        if self.db is not None:
            return self.db.remove(self, options)
        raise DocNeverAddedError("Cannot remove an object that was never added")

def doc_from_string(char *data):
    cdef ESTDOC *doc_p
    cdef Document doc
    doc_p = est_doc_new_from_draft(data)
    doc = Document()
    doc.estdoc = doc_p
    return doc

cdef class Condition:
    cdef ESTCOND *estcond

    def __new__(self):
        self.estcond = est_cond_new()

    def set_phrase(self, phrase):
        est_cond_set_phrase(self.estcond, phrase)

    def add_attr(self, attr):
        est_cond_add_attr(self.estcond, attr)

    def set_order(self, order):
        est_cond_set_order(self.estcond, order)

    def set_max(self, int max):
        est_cond_set_max(self.estcond, max)

    def set_options(self, int options):
        est_cond_set_options(self.estcond, options)

    def __dealloc__(self):
        est_cond_delete(self.estcond)

cdef class Database:
    cdef ESTDB *estdb
    cdef int _ecode

    def __new__(self, name, omode=ESTDBWRITER | ESTDBCREAT):
        self.estdb = est_db_open(name, omode, &self._ecode)

    def close(self):
        if self.estdb != NULL:
            est_db_close(self.estdb, &self._ecode)
            self.estdb = NULL

    def _check(self):
        """
        Check that the connection has not been close already.
        """
        if self.estdb == NULL:
            raise HyperEstraierException('Database is closed.')

    property name:
        def __get__(self):
            self._check()
            return est_db_name(self.estdb)

    property size:
        def __get__(self):
            self._check()
            return est_db_size(self.estdb)

    property ecode:
        def __get__(self):
            self._check()
            return est_err_msg(self._ecode)

    property efatal:
        def __get__(self):
            self._check()
            return bool(est_db_fatal(self.estdb))

    def __len__(self):
        self._check()
        return est_db_doc_num(self.estdb)

    def put_doc(self, Document doc):
        self._check()
        est_db_put_doc(self.estdb, doc.estdoc, ESTPDCLEAN)
        doc.db = self
        Py_INCREF(self) # Incref

    def get_doc(self, int id, int options = 0):
        cdef ESTDOC *doc_p
        cdef Document doc
        self._check()
        doc_p = est_db_get_doc(self.estdb, id, options)
        if doc_p != NULL:
            doc = Document()
            doc.estdoc = doc_p
            doc.db = self
            Py_INCREF(self) # Incref
            return doc
        return None

    def get_doc_by_uri(self, uri):
        cdef int id
        self._check()
        id = est_db_uri_to_id(self.estdb, uri)
        return self.get_doc(id)

    def flush(self, int max = 0):
        self._check()
        if est_db_flush(self.estdb, max):
            return True
        raise DBFlushError("Wasn't able to flush the database.")

    def sync(self):
        self._check()
        if est_db_sync(self.estdb):
            return True
        raise DBSyncError("Wasn't able to sync the database.")

    def optimize(self, int options = 0):
        self._check()
        if est_db_optimize(self.estdb, options):
            return True
        raise DBOptimizeError("Wasn't able to optimize the database.")

    def search(self, phrase=None, simple=False):
        self._check()
        return Search(self, phrase, simple)

    def commit(self, Document doc):
        self._check()
        if est_db_edit_doc(self.estdb, doc.estdoc):
            return True
        raise DBEditError("Error while editing an object")

    def remove(self, Document doc , int options = ESTODCLEAN):
        self._check()
        if est_db_out_doc(self.estdb, doc.id, options):
            return True
        raise DBRemoveError("Error while removing an object")

cdef class Search:
    """
    Search provides a way to search for, order and limit indexed documents.

    A Search instance is never created directly, an instance is created and
    returned by calling database.search().

    Search supports the iterator and len protocols as expected, as well as index
    and slice __getitem__ access.

    Search also provides methods to modify the set of documents returned by the
    search.
    """

    cdef Database database
    cdef Condition condition
    cdef int results_len
    cdef int *results

    def __new__(self, database, phrase, simple):
        self.database = database
        self.condition = Condition()
        if phrase is not None:
            self.condition.set_phrase(phrase)
        if simple:
            self.condition.set_options(ESTCONDSIMPLE)

    def __dealloc__(self):
        if self.results != NULL:
            free(self.results)
            self.results = NULL

    def max(self, max):
        """
        Set the maximum number of documents returned by the search.
        """
        self.condition.set_max(max)
        return self

    def add(self, expr):
        """
        Add an attribute expression.
        """
        self.condition.add_attr(expr)
        return self

    def order(self, expr):
        """
        Set the ordering expression.
        """
        self.condition.set_order(expr)
        return self

    def __getitem__(self, s):
        """
        Return an item or slice of the results as one or a sequence of
        Document instances.
        """
        self.prepare()
        if isinstance(s, slice):
            return SearchIterator(self, *s.indices(self.results_len))
        else:
            return self.doc_at(s)

    def __len__(self):
        """
        Return the number of documents found by the search.
        """
        self.prepare()
        return self.results_len

    def __iter__(self):
        """
        Support the iterator protocol.
        """
        self.prepare()
        return SearchIterator(self, 0, self.results_len, 1)

    def prepare(self):
        """
        Prepare the finder for returning results. This executes the search if
        necessary and initialises any attributes needed to support further
        calls.
        """
        if self.results == NULL:
            self.results = est_db_search(
                self.database.estdb,
                self.condition.estcond,
                &self.results_len,
                NULL)

    def doc_at(self, pos):
        """
        Return the document at the given index position.
        """
        if pos < 0 or pos >= self.results_len:
            raise IndexError()
        docid = self.results[pos]
        return self.database.get_doc(docid)

class SearchIterator(object):

    def __init__(self, Search search, start, stop, stride):
        self.search = search
        self.start = start
        self.stop = stop
        self.stride = stride
        self.current = start

    def __iter__(self):
        return self

    def next(self):
        # Check there's actually something to iterate
        if self.start == self.stop:
            raise StopIteration()
        # Check the direction of the stride
        if (self.stop>self.start and self.stride<0) or (self.stop<self.start and self.stride>0):
            raise StopIteration()
        # Check if we've reached the stop index
        if self.current == self.stop:
            raise StopIteration()
        doc = self.search.doc_at(self.current)
        self.current = self.current + self.stride
        return doc

