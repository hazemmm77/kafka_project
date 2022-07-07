#include "Python.h"
#include "structmember.h"

#include <sys/time.h>
#include <time.h>
#include <stdio.h>

#include "libnumarray.h"
#include "tc.h"

char *_teacup__doc__ = "";

static PyObject *pTimingDict;
static double calibration_nested, calibration_top;
static long total_starts;

#define CALIBRATIONS 1000000

#define tc_get(x)   _teacup_get(__FILE__, x)

#define TEACUP_AVG(t) ((t->accum/t->start_count)-(calibration_nested*t->cycles)-calibration_top)

#define DEFERRED_ADDRESS(ADDR) 0

staticforward PyTypeObject _teacup_type;

static PyObject *
_teacup_average_time_get(PyTeaCupObject *self)
{
	return PyFloat_FromDouble(TEACUP_AVG(self));
}

static PyObject *
_teacup_start_count_get(PyTeaCupObject *self)
{
	return PyInt_FromLong(self->start_count);
}

static PyObject *
_teacup_cycles_get(PyTeaCupObject *self)
{
	return PyInt_FromLong(self->cycles);
}

static PyObject *
_teacup_start_get(PyTeaCupObject *self)
{
	return PyFloat_FromDouble(self->start);
}

static PyObject *
_teacup_stop_get(PyTeaCupObject *self)
{
	return PyFloat_FromDouble(self->stop);
}

static PyObject *
_teacup_accum_get(PyTeaCupObject *self)
{
	return PyFloat_FromDouble(self->accum);
}

static PyGetSetDef 
_teacup_getsets[] = {
	{"start_count", 
	 (getter)_teacup_start_count_get, 
	 (setter)NULL,
	 "start_count -- count of starts for this tag"}, 
	{"cycles", 
	 (getter)_teacup_cycles_get, 
	 (setter)NULL,
	 "cycles -- count of start/stop cycles for all nested tags"}, 
	{"start", 
	 (getter)_teacup_start_get, 
	 (setter)NULL,
	 "last start time"}, 
	{"stop", 
	 (getter)_teacup_stop_get, 
	 (setter)NULL,
	 "last stop time"}, 
	{"accum", 
	 (getter)_teacup_accum_get, 
	 (setter)NULL,
	 "accumulated time"}, 
	{"average_time", 
	 (getter)_teacup_average_time_get, 
	 NULL, 
	 "accumulated time / start_count"}, 
	{0},
};

static void
_teacup_clear(PyTeaCupObject *self)
{
	self->start_count = 0;
	self->start = 0;
	self->stop = 0;
	self->accum = 0;
	self->cycles = 0;
	self->entries = 0;
}

static PyObject *
_teacup_new(PyTypeObject *type, PyObject  *args, PyObject *kwds)
{
	PyTeaCupObject *self = 
		(PyTeaCupObject *) PyType_GenericNew(type, args, kwds);
	if (!self) return NULL;
	_teacup_clear(self);
	return (PyObject *) self;
}

static int
_teacup_type_init(PyObject *self, PyObject  *args, PyObject *kwds)
{
	PyTeaCupObject *me = (PyTeaCupObject *) self;
	if (!PyArg_ParseTuple(args, "|ddd:_teacup_init", 
			      &me->start, &me->stop, &me->accum))
		return -1;
	return 0;
}

static void
_teacup_dealloc(PyObject *self)
{
	self->ob_type->tp_free(self);
}

static PyMethodDef _teacup_methods[] = {
	/* {"compute", (PyCFunction)_Py_teacup_compute, METH_VARARGS,
	   "compute(self, indices, shape)  computes one block at 'indices' with 'shape'"}, */
	{NULL,	NULL},
};

static PyObject *
_Py_teacup_repr(PyObject *self)
{
	PyTeaCupObject *me = (PyTeaCupObject *) self;
	char s[128];
	snprintf(s, sizeof(s), 
		 "count:%6lu  avg_usec:%8.2f", 
		 me->start_count, TEACUP_AVG(me));
	return PyString_FromString(s);
}

static PyTypeObject _teacup_type = {
	PyObject_HEAD_INIT(DEFERRED_ADDRESS(&PyType_Type))
	0,
	"teacup._teacup",
	sizeof(PyTeaCupObject),
	0,
	_teacup_dealloc,	                /* tp_dealloc */
	0,					/* tp_print */
	0,					/* tp_getattr */
	0,					/* tp_setattr */
	0,					/* tp_compare */
	_Py_teacup_repr,			/* tp_repr */
	0,					/* tp_as_number */
	0,                                      /* tp_as_sequence */
	0,			                /* tp_as_mapping */
	0,					/* tp_hash */
	0,					/* tp_call */
	0,					/* tp_str */
	0,					/* tp_getattro */
	0,					/* tp_setattro */
	0,					/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT |
	Py_TPFLAGS_BASETYPE,                    /* tp_flags */
	0,					/* tp_doc */
	0,					/* tp_traverse */
	0,					/* tp_clear */
	0,					/* tp_richcompare */
	0,					/* tp_weaklistoffset */
	0,					/* tp_iter */
	0,					/* tp_iternext */
	_teacup_methods,			/* tp_methods */
	0,					/* tp_members */
	_teacup_getsets,			/* tp_getset */
	0,                                      /* tp_base */
	0,					/* tp_dict */
	0,					/* tp_descr_get */
	0,					/* tp_descr_set */
	0,					/* tp_dictoffset */
	_teacup_type_init,	                /* tp_init */
	0, 		                        /* tp_alloc */
	_teacup_new,				/* tp_new */
};

#if defined(MEASURE_TIMING)
unsigned long usec(void)
{
	struct timeval tp;
	gettimeofday(&tp, NULL);
	return (1000000*tp.tv_sec + tp.tv_usec);
}
#else
#define usec() 0
#endif

static PyTeaCupObject *
_teacup_get(char *file, char *tag)
{
	PyTeaCupObject *tc;
	PyObject *key = Py_BuildValue("(ss)", file, tag);
	if (!key) 
		Py_FatalError("libteacup: Can't create key.");
	tc  = (PyTeaCupObject *) PyDict_GetItem(pTimingDict, key);
	if (!tc) {
		tc = (PyTeaCupObject *) _teacup_new(&_teacup_type, NULL, NULL);
		if (!tc) 
			Py_FatalError("libteacup: Can't create a new teacup object.");
		if (PyDict_SetItem(pTimingDict, key, (PyObject *)tc) < 0)
			Py_FatalError("libteacup: Can't insert new teacup in timing dict.");
	}
	Py_INCREF(tc);
	return tc;
}

static void *
_teacup_start_clock(char *file, char *tag, void  *cache)
{
	if (cache) {
		double t0 = usec();
		PyTeaCupObject *tc = (PyTeaCupObject *) cache;
		tc->start = t0;
		++ tc->start_count;
		if (++tc->entries == 1)  
			tc->starts_on_entry = ++total_starts;
		return cache;
	} else {
		return (void *) _teacup_get(file, tag);
	}
}

static void *
_teacup_stop_clock(char *file, char *tag, void *cache)
{
	if (cache) {
		PyTeaCupObject *tc = (PyTeaCupObject *) cache;
		if (!tc) return NULL;
		if (tc->entries-- == 1)
			tc->cycles = total_starts - tc->starts_on_entry;
		tc->stop = usec();
		tc->accum += tc->stop - tc->start;
		return cache;
	} else {
		return (void *) _teacup_get(file, tag);
	}
} 

static int
_teacup_calibrate(long N)
{
	PyTeaCupObject *nested = tc_get("calibration nested");
	PyTeaCupObject *top    = tc_get("calibration top");
	int i;
	
	for(i=0; i<N; i++) {
		tc_start_clock("calibration nested");
		   tc_start_clock("calibration top");
		   tc_stop_clock("calibration top");
		tc_stop_clock("calibration nested");
	}

	calibration_top    = top->accum / top->start_count;
	calibration_nested = nested->accum / nested->start_count 
		- calibration_top;
	return 0;
}

static PyObject *
_Py_teacup_calibrate(PyObject *module, PyObject *args)
{
	long N = CALIBRATIONS;

	if (!PyArg_ParseTuple(args, "|i:calibrate", &N))
		return NULL;

	_teacup_calibrate(N);

	Py_INCREF(Py_None);
	return Py_None;
}

static int
_teacup_reset(void)
{
	int pos=0;
	PyObject *key, *value;
	while(PyDict_Next(pTimingDict, &pos, &key, &value))
		_teacup_clear((PyTeaCupObject *)value);
	total_starts = 0;
	calibration_nested = calibration_top = 0;
	if (_teacup_calibrate(CALIBRATIONS) < 0)
		return -1;
	return 0;
}

static PyObject *
_Py_teacup_reset(PyObject *module, PyObject *args)
{
	if (!PyArg_ParseTuple(args, ":reset"))
		return NULL;

	if (_teacup_reset() < 0)
		return NULL;

	Py_INCREF(Py_None);
	return Py_None;
}

static int
libteacup_init(void)
{
	PyObject *m;
	
	_teacup_type.tp_alloc = PyType_GenericAlloc;

	if (PyType_Ready(&_teacup_type) < 0)
		return -1;
	
	m = PyImport_ImportModule("numarray.libteacup");
	if (m == NULL) return -1;
	
	Py_INCREF(&_teacup_type);
	if (PyModule_AddObject(m, "teacup",
			       (PyObject *) &_teacup_type) < 0)
		return -1;

	pTimingDict = PyDict_New();
	if (!pTimingDict) return -1;

	if (_teacup_calibrate(CALIBRATIONS) < 0) return -1;

	Py_DECREF(m);
	return 0;
}

static PyObject *
_Py_teacup_set_calibration(PyObject *module, PyObject *args)
{
	if (!PyArg_ParseTuple(args, "dd:set_calibration", 
			      &calibration_top, &calibration_nested))
		return NULL;
	Py_INCREF(Py_None);
	return Py_None;
}

static PyObject *
_Py_teacup_get_calibration(PyObject *module, PyObject *args)
{
	if (!PyArg_ParseTuple(args, ":get_calibration"))
		return NULL;
	return Py_BuildValue("(dd)", calibration_top, calibration_nested);
}

static PyObject *
_Py_teacup_get_timings(PyObject *module, PyObject *args)
{
	if (!PyArg_ParseTuple(args, ":get_timings"))
		return NULL;
	Py_INCREF(pTimingDict);
	return pTimingDict;
}


static PyMethodDef 
_libteacupMethods[] = {
	{"get_calibration", _Py_teacup_get_calibration, METH_VARARGS,
	 "get_calibration() -> top, nested"},
	{"set_calibration", _Py_teacup_set_calibration, METH_VARARGS,
	 "set_calibration(top, nested)"},
	{"get_timings", _Py_teacup_get_timings, METH_VARARGS,
	 "get_timings() ->    dictionary of timing information"},	
	{"reset", _Py_teacup_reset, METH_VARARGS,
	 "reset()"},
	{"calibrate", _Py_teacup_calibrate, METH_VARARGS,
	 "calibrate(N)"},
	{NULL,		NULL}		/* sentinel */
};

#define METHOD_TABLE_EXISTS 1
