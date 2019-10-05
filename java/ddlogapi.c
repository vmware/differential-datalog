/* Wrapper that converts a JNI C interface to calls to the native ddlog C API */

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "ddlogapi_DDlogAPI.h"
#include "ddlog.h"
#include "ddlog_log.h"

/* Error message returned by DDlog. */
_Thread_local char* err_msg = NULL;

/*
 * `vasprintf` implementation copied from
 * https://github.com/littlstar/asprintf.c (MIT license)
 */
static int _vasprintf (char **str, const char *fmt, va_list args) {
  int size = 0;
  va_list tmpa;

  va_copy(tmpa, args);
  size = vsnprintf(NULL, 0, fmt, tmpa);
  va_end(tmpa);

  if (size < 0) { return -1; }

  // alloc with size plus 1 for `\0'
  *str = (char *) malloc(size + 1);
  if (NULL == *str) { return -1; }

  size = vsprintf(*str, fmt, args);
  return size;
}

/*
 * Callback invoked by DDlog on error.
 *
 * Record error message in a thread-local variable so that it can later be
 * wrapped in an exception.
 */
static void eprintln(const char* msg) {
    if (err_msg) {
        free(err_msg);
    }
    err_msg = strdup(msg);
}

/*
 * Throw a `DDlogException`. If `m` is NULL, uses error message in `err_msg`.
 */
static void throwDDlogException(JNIEnv* env, const char* m) {
    const char * msg = m ? m : (err_msg ? err_msg : "Unknown error");
    jclass eclass = (*env)->FindClass(env, "ddlogapi/DDlogException");
    if (eclass) {
        (*env)->ThrowNew(env, eclass, msg);
    } else {
        eclass = (*env)->FindClass(env, "java/lang/Exception");
        (*env)->ThrowNew(env, eclass, msg);
    }
    err_msg = NULL;
}

static void throwIOException(JNIEnv* env, const char * fmt, ...) {
    char *msg;
    va_list ap;
    va_start(ap, fmt);
    int nbytes = _vasprintf(&msg, fmt, ap);
    va_end(ap);

    jclass eclass = (*env)->FindClass(env, "java/io/IOException");
    if (nbytes >= 0) {
        (*env)->ThrowNew(env, eclass, msg);
        free(msg);
    } else {
        (*env)->ThrowNew(env, eclass,
                         "IO exception occurred, but the error message could not be displayed.");
    }
}

static void throwOutOfMemException(JNIEnv* env, const char * fmt, ...) {
    char *msg;
    va_list ap;
    va_start(ap, fmt);
    int nbytes = _vasprintf(&msg, fmt, ap);
    va_end(ap);

    if (nbytes >= 0) {
        throwDDlogException(env, msg);
        free(msg);
    } else {
        throwDDlogException(env,
                "Out-of-memory exception occurred, but the error message could not be displayed.");
    }
}


/* The _1 in all the function names below
   is the JNI translation of the _ character from Java */

// Describes a callback to an instance method.
struct CallbackInfo {
    JNIEnv* env;  // may be NULL if the callback is invoked from a different thread.
    JavaVM* jvm;  // Used to retrieve a new env when it is NULL.
    // Class containing method to be called.  A global JNI reference.
    jclass  cls;
    // Instance object of the class.  A global JNI reference.
    jobject obj;
    // Handle to the method to call.
    jmethodID method;
};

// Debugging code
static void printClass(JNIEnv* env, jobject obj) {
    jclass cls = (*env)->GetObjectClass(env, obj);
    // First get the class object
    jmethodID mid = (*env)->GetMethodID(env, cls, "getClass", "()Ljava/lang/Class;");
    jobject clsObj = (*env)->CallObjectMethod(env, obj, mid);
    // Now get the class object's class descriptor
    cls = (*env)->GetObjectClass(env, clsObj);
    // Find the getName() method on the class object
    mid = (*env)->GetMethodID(env, cls, "getName", "()Ljava/lang/String;");
    // Call the getName() to get a jstring object back
    jstring strObj = (jstring)(*env)->CallObjectMethod(env, clsObj, mid);
    const char* str = (*env)->GetStringUTFChars(env, strObj, NULL);
    // Print the class name
    fprintf(stderr, "Class is: %s\n", str);
    (*env)->ReleaseStringUTFChars(env, strObj, str);
}

static struct CallbackInfo* createCallbackByName(JNIEnv* env, jobject obj, const char* method, const char* signature) {
    if (method == NULL)
        return NULL;
    struct CallbackInfo* cbinfo = malloc(sizeof(struct CallbackInfo));
    if (cbinfo == NULL)
        return NULL;
    jint error = (*env)->GetJavaVM(env, &cbinfo->jvm);
    cbinfo->env = NULL;
    cbinfo->obj = (*env)->NewGlobalRef(env, obj);
    jclass thisClass = (*env)->GetObjectClass(env, cbinfo->obj);
    cbinfo->cls = (jclass)(*env)->NewGlobalRef(env, thisClass);
    jmethodID methodId = (*env)->GetMethodID(env, cbinfo->cls, method, signature);

    if (methodId == NULL)
        return NULL;
    cbinfo->method = methodId;
    return cbinfo;
}

static struct CallbackInfo* createCallback(JNIEnv* env, jobject obj, jstring method, const char* signature) {
    if (method == NULL)
        return NULL;

    const char* methodstr = (*env)->GetStringUTFChars(env, method, NULL);
    struct CallbackInfo *cbinfo = createCallbackByName(env, obj, methodstr, signature);
    (*env)->ReleaseStringUTFChars(env, method, methodstr);
    return cbinfo;
}

static void deleteCallback(struct CallbackInfo* cbinfo) {
    if (cbinfo == NULL)
        return;
    JNIEnv* env;
    if (cbinfo->env == NULL)
        (*cbinfo->jvm)->AttachCurrentThread(cbinfo->jvm, (void**)&env, NULL);
    else
        env = cbinfo->env;
    (*env)->DeleteGlobalRef(env, cbinfo->cls);
    (*env)->DeleteGlobalRef(env, cbinfo->obj);
    if (cbinfo->env == NULL)
        (*cbinfo->jvm)->DetachCurrentThread(cbinfo->jvm);
    free(cbinfo);
}

void commit_callback(void* callbackInfo, table_id tableid, const ddlog_record* rec, ssize_t w) {
    struct CallbackInfo* cbi = (struct CallbackInfo*)callbackInfo;
    if (cbi == NULL || cbi->jvm == NULL)
        return;
    JNIEnv* env;
    (*cbi->jvm)->AttachCurrentThreadAsDaemon(cbi->jvm, (void**)&env, NULL);
    (*env)->CallVoidMethod(
        env, cbi->obj, cbi->method, (jint)tableid, (jlong)rec, (jlong)w);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1run(
    JNIEnv *env, jobject obj, jboolean storeData, jint workers, jstring callback) {
    jlong handle = 0;

    if (workers <= 0)
        workers = 1;

    if (callback == NULL) {
        handle = (jlong)ddlog_run((unsigned)workers, storeData, NULL, 0, eprintln);
        if (handle == 0) {
            throwDDlogException(env, NULL);
        }
        return handle;
    }

    struct CallbackInfo* cbinfo = createCallback(env, obj, callback, "(IJJ)V");
    if (cbinfo == NULL)
        return 0;

    // store the callback pointer in the parent Java object
    jclass thisClass = (*env)->GetObjectClass(env, obj);
    jfieldID callbackHandle = (*env)->GetFieldID(env, thisClass, "callbackHandle", "J");
    if (callbackHandle == NULL)
        return 0;
    (*env)->SetLongField(env, obj, callbackHandle, (jlong)cbinfo);

    handle = (jlong)ddlog_run((unsigned)workers, storeData, commit_callback, (uintptr_t)cbinfo, eprintln);
    if (handle == 0) {
        throwDDlogException(env, NULL);
    }

    return handle;
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDlogAPI_ddlog_1record_1commands(
    JNIEnv *env, jobject obj, jlong handle, jstring filename, jboolean append) {
    int ret;
    int fd;

    const char *c_filename = (*env)->GetStringUTFChars(env, filename, NULL);
    if (c_filename == NULL) {
        return -1;
    }
    fd = open(c_filename, O_CREAT | O_WRONLY | (append ? O_APPEND : O_TRUNC),
              S_IRUSR | S_IWUSR);
    (*env)->ReleaseStringUTFChars(env, filename, c_filename);

    if (fd < 0) {
        throwIOException(env, "Failed to open file %s. Error code: %d", c_filename, fd);
        return fd;
    } else if ((ret = ddlog_record_commands((ddlog_prog)handle, fd))) {
        close(fd);
        throwDDlogException(env, NULL);
        return ret;
    } else {
        return fd;
    }
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_ddlog_1stop_1recording(
    JNIEnv *env, jobject obj, jlong handle, jint fd) {
    if (ddlog_record_commands((ddlog_prog)handle, -1) < 0) {
        throwDDlogException(env, NULL);
    }
    close(fd);
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_ddlog_1dump_1input_1snapshot(
    JNIEnv *env, jobject obj, jlong handle, jstring filename, jboolean append) {
    int ret;
    int fd;

    const char *c_filename = (*env)->GetStringUTFChars(env, filename, NULL);
    if (c_filename == NULL) {
        return;
    }
    fd = open(c_filename, O_CREAT | O_WRONLY | (append ? O_APPEND : O_TRUNC),
              S_IRUSR | S_IWUSR);

    if (fd < 0) {
        throwIOException(env, "Failed to open file %s. Error code: %d", c_filename, fd);
    } else {
        if (ddlog_dump_input_snapshot((ddlog_prog)handle, fd) < 0) {
            throwDDlogException(env, NULL);
        }
        close(fd);
    }

    (*env)->ReleaseStringUTFChars(env, filename, c_filename);
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_ddlog_1stop(
    JNIEnv *env, jobject obj, jlong handle, jlong callbackHandle) {
    // Delete the callback pointer stored in the parent Java object
    deleteCallback((void*)callbackHandle);
    if (ddlog_stop((ddlog_prog)handle) < 0) {
        throwDDlogException(env, NULL);
    }
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_ddlog_1transaction_1start(
    JNIEnv * env, jobject obj, jlong handle) {
    int code = ddlog_transaction_start((ddlog_prog)handle);
    if (code < 0) {
        throwDDlogException(env, NULL);
    }
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_ddlog_1transaction_1commit(
    JNIEnv * env, jobject obj, jlong handle) {
    if (ddlog_transaction_commit((ddlog_prog)handle) < 0) {
        throwDDlogException(env, NULL);
    }
}

void commit_dump_callback(void* callbackInfo, table_id tableid, const ddlog_record* rec, bool polarity) {
    struct CallbackInfo* cbi = (struct CallbackInfo*)callbackInfo;
    if (cbi == NULL || cbi->jvm == NULL)
        return;
    JNIEnv* env;
    (*cbi->jvm)->AttachCurrentThreadAsDaemon(cbi->jvm, (void**)&env, NULL);
    (*env)->CallVoidMethod(
        env, cbi->obj, cbi->method, (jint)tableid, (jlong)rec, (jboolean)polarity);
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_ddlog_1transaction_1commit_1dump_1changes(
    JNIEnv * env, jobject obj, jlong handle, jstring callback) {

    if (callback == NULL) {
        if (ddlog_transaction_commit_dump_changes((ddlog_prog)handle, NULL, 0) < 0) {
            throwDDlogException(env, NULL);
            return;
        }
    }

    struct CallbackInfo* cbinfo = createCallback(env, obj, callback, "(IJZ)V");
    if (cbinfo == NULL)
        return;

    if (ddlog_transaction_commit_dump_changes(
                (ddlog_prog)handle,
                commit_dump_callback,
                (uintptr_t)cbinfo) < 0) {
        throwDDlogException(env, NULL);
    };
    free(cbinfo);
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_ddlog_1transaction_1commit_1dump_1changes_1to_1flatbuf(
    JNIEnv * env, jobject obj, jlong handle, jobject fbdescr) {
    unsigned char *buf_addr = NULL;
    size_t buf_size         = 0;
    size_t buf_capacity     = 0;
    size_t buf_offset       = 0;

    jclass cls = (*env)->FindClass(env, "ddlogapi/DDlogAPI$FlatBufDescr");
    if (cls == NULL) {
        return;
    }
    jmethodID setMethod = (*env)->GetMethodID(env, cls, "set", "(Ljava/nio/ByteBuffer;JJ)V");
    if(setMethod == NULL) {
        return;
    }

    if (ddlog_transaction_commit_dump_changes_to_flatbuf(
            (ddlog_prog)handle,
            &buf_addr,
            &buf_size,
            &buf_capacity,
            &buf_offset) < 0) {
        throwDDlogException(env, NULL);
        return;
    }

    jobject direct_buf = (*env)->NewDirectByteBuffer(env, buf_addr + buf_offset, (jlong)(buf_capacity - buf_offset));

    (*env)->CallVoidMethod(env, fbdescr, setMethod,
            direct_buf, (jlong)buf_size, (jlong)buf_offset);
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_ddlog_1flatbuf_1free(
    JNIEnv * env, jobject obj, jobject buf, jlong size, jlong offset)
{
    unsigned char* addr = (unsigned char*)(((size_t)(*env)->GetDirectBufferAddress(env, buf)) - offset);
    if (addr == NULL) {
        return;
    }

    ddlog_flatbuf_free(
            addr,
            (size_t)size,
            ((size_t)(*env)->GetDirectBufferCapacity(env, buf)) + offset);
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_ddlog_1transaction_1rollback(
    JNIEnv * env, jobject obj, jlong handle) {
    if (ddlog_transaction_rollback((ddlog_prog)handle) < 0) {
        throwDDlogException(env, NULL);
    }
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_ddlog_1apply_1updates(
    JNIEnv *env, jclass obj, jlong progHandle, jlongArray commandHandles) {
    jlong *a = (*env)->GetLongArrayElements(env, commandHandles, NULL);
    size_t size = (*env)->GetArrayLength(env, commandHandles);
    ddlog_cmd** updates = malloc(sizeof(ddlog_cmd*) * size);
    if (updates == NULL) {
        throwOutOfMemException(env, "Could not allocate buffer for commands.");
        return;
    }
    for (size_t i = 0; i < size; i++)
        updates[i] = (ddlog_cmd*)a[i];
    if (ddlog_apply_updates((ddlog_prog)progHandle, updates, size) < 0) {
        throwDDlogException(env, NULL);
    }
    (*env)->ReleaseLongArrayElements(env, commandHandles, a, 0);
    free(updates);
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_ddlog_1apply_1updates_1from_1flatbuf(
    JNIEnv *env, jclass obj, jlong progHandle, jbyteArray bytes, jint position) {
    jbyte *buf = (*env)->GetByteArrayElements(env, bytes, NULL);
    size_t size = (*env)->GetArrayLength(env, bytes);

    if (ddlog_apply_updates_from_flatbuf(
        (ddlog_prog)progHandle, ((const unsigned char *) buf) + position, size) < 0) {
        throwDDlogException(env, NULL);
    };

    (*env)->ReleaseByteArrayElements(env, bytes, buf, JNI_ABORT);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDlogAPI_ddlog_1clear_1relation(
    JNIEnv *env, jclass obj, jlong progHandle, jint relid) {
    int result = ddlog_clear_relation((ddlog_prog)progHandle, relid);
    return (jint)result;
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1table_1id(
    JNIEnv *env, jclass class, jstring table) {
    const char* tbl = (*env)->GetStringUTFChars(env, table, NULL);
    table_id id = ddlog_get_table_id(tbl);
    (*env)->ReleaseStringUTFChars(env, table, tbl);
    return (jint)id;
}

JNIEXPORT jstring JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1table_1name(
    JNIEnv *env, jclass class, jint id) {
    const char* table = ddlog_get_table_name(id);
    jstring result = (*env)->NewStringUTF(env, table);
    ddlog_string_free(table);
    return result;
}

bool dump_callback(uintptr_t callbackInfo, const ddlog_record* rec) {
    struct CallbackInfo* cbi = (struct CallbackInfo*)callbackInfo;
    JNIEnv* env = cbi->env;
    assert(env);
    jboolean result = (*env)->CallBooleanMethod(env, cbi->obj, cbi->method, (jlong)rec);
    return (bool)result;
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_dump_1table(
    JNIEnv *env, jobject obj, jlong progHandle, jint table, jstring callback) {
    struct CallbackInfo* cbinfo = createCallback(env, obj, callback, "(J)Z");
    if (cbinfo == NULL)
        return;
    cbinfo->env = env;  // the dump_callback will be called on the same thread
    if (ddlog_dump_table((ddlog_prog)progHandle, table, dump_callback, (uintptr_t)cbinfo) < 0) {
        throwDDlogException(env, NULL);
    }
    free(cbinfo);
}

JNIEXPORT jstring JNICALL Java_ddlogapi_DDlogAPI_ddlog_1profile(
    JNIEnv *env, jobject obj, jlong progHandle) {
    char* profile = ddlog_profile((ddlog_prog)progHandle);
    jstring result = (*env)->NewStringUTF(env, profile);
    ddlog_string_free(profile);
    return result;
}

JNIEXPORT void JNICALL Java_ddlogapi_DDlogAPI_ddlog_1enable_1cpu_1profiling(
    JNIEnv *env, jobject obj, jlong progHandle, jboolean enable) {
    if (ddlog_enable_cpu_profiling((ddlog_prog)progHandle, enable) < 0) {
        throwDDlogException(env, NULL);
    }
}

void log_callback(uintptr_t callbackInfo, int level, const char *msg) {
    struct CallbackInfo* cbi = (struct CallbackInfo*)callbackInfo;
    JNIEnv* env;
    (*cbi->jvm)->AttachCurrentThreadAsDaemon(cbi->jvm, (void**)&env, NULL);
    assert(env);
    jstring jmsg = (*env)->NewStringUTF(env, msg);
    (*env)->CallVoidMethod(env, cbi->obj, cbi->method, jmsg, (jint) level);
}

/* This function sets up a new logging callback for the given module and
 * then deallocates the old `CallbackInfo`, if any.
 *
 * Returns 0 if either `callback` is NULL or `callback` is not NULL and
 * installing the new callback fails.
 */
JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1log_1replace_1callback(
    JNIEnv *env, jobject obj, jint module, jlong old_cbinfo, jobject callback, jint max_level) {

    if (callback == NULL) {
        ddlog_log_set_callback(module, NULL, 0, max_level);
        deleteCallback((struct CallbackInfo*)old_cbinfo);
        return 0;
    }

    struct CallbackInfo* cbinfo = createCallbackByName(env, callback, "accept", "(Ljava/lang/Object;I)V");
    if (cbinfo == NULL) {
        deleteCallback((struct CallbackInfo*)old_cbinfo);
        return 0;
    }

    ddlog_log_set_callback(module, log_callback, (uintptr_t)cbinfo, max_level);

    deleteCallback((struct CallbackInfo*)old_cbinfo);
    return (jlong)cbinfo;
}


JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1bool(
    JNIEnv *env, jclass obj, jboolean b) {
    return (jlong)ddlog_bool(b);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1i64(
    JNIEnv *env, jclass obj, jlong l) {
    return (jlong)ddlog_i64(l);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1int(
    JNIEnv *env, jclass obj, jbyteArray v) {
    jboolean isCopy;
    jbyte* b = (*env)->GetByteArrayElements(env, v, &isCopy);
    jsize size = (*env)->GetArrayLength(env, v);
    jlong res = (jlong)ddlog_int((unsigned char*)b, size);
    (*env)->ReleaseByteArrayElements(env, v, b, JNI_ABORT);
    return res;
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1string(
    JNIEnv *env, jclass obj, jstring s) {
    const char* str = (*env)->GetStringUTFChars(env, s, NULL);
    ddlog_record* result = ddlog_string(str);
    (*env)->ReleaseStringUTFChars(env, s, str);
    return (jlong)result;
}

jlong create_from_vector(JNIEnv *env, jclass obj, jlongArray handles,
                         ddlog_record* (*creator)(ddlog_record** fields, size_t size)) {
    jlong *a = (*env)->GetLongArrayElements(env, handles, NULL);
    if (a == NULL)
        return -1;
    jsize len = (*env)->GetArrayLength(env, handles);
    ddlog_record** fields = malloc(len * sizeof(ddlog_record*));
    if (fields == NULL) {
        throwOutOfMemException(env, "Could not allocate buffer for %d value.", len);
        return -1;
    }
    for (size_t i = 0; i < len; i++)
        fields[i] = (ddlog_record*)a[i];
    ddlog_record* result = creator(fields, (size_t)len);
    (*env)->ReleaseLongArrayElements(env, handles, a, 0);
    free(fields);
    return (jlong)result;
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1tuple(
    JNIEnv *env, jclass obj, jlongArray handles) {
    return create_from_vector(env, obj, handles, ddlog_tuple);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1vector(
    JNIEnv *env, jclass obj, jlongArray handles) {
    return create_from_vector(env, obj, handles, ddlog_vector);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1set(
    JNIEnv *env, jclass obj, jlongArray handles) {
    return create_from_vector(env, obj, handles, ddlog_set);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1map(
    JNIEnv *env, jclass obj, jlongArray handles) {
    return create_from_vector(env, obj, handles, ddlog_map);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1pair(
    JNIEnv *env, jclass obj, jlong h1, jlong h2) {
    return (jlong)ddlog_pair((ddlog_record*)h1, (ddlog_record*)h2);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1struct(
    JNIEnv *env, jclass obj, jstring s, jlongArray handles) {
    const char* str = (*env)->GetStringUTFChars(env, s, NULL);
    jsize len = (*env)->GetArrayLength(env, handles);
    jlong *a = (*env)->GetLongArrayElements(env, handles, NULL);
    if (a == NULL)
        return -1;
    ddlog_record** fields = malloc(len * sizeof(ddlog_record*));
    if (fields == NULL) {
        throwOutOfMemException(env, "Could not allocate buffer for %d fields.", len);
        return -1;
    }
    for (size_t i = 0; i < len; i++)
        fields[i] = (ddlog_record*)a[i];
    ddlog_record* result = ddlog_struct(str, fields, len);
    (*env)->ReleaseLongArrayElements(env, handles, a, 0);
    free(fields);
    (*env)->ReleaseStringUTFChars(env, s, str);
    return (jlong)result;
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDlogAPI_ddlog_1is_1bool(
    JNIEnv *env, jclass obj, jlong handle) {
    return (jboolean)ddlog_is_bool((ddlog_record*)handle);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1bool(
    JNIEnv *env, jclass obj, jlong handle) {
    return (jboolean)ddlog_get_bool((ddlog_record*)handle);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDlogAPI_ddlog_1is_1int(
    JNIEnv *env, jclass obj, long handle) {
    return (jboolean)ddlog_is_int((ddlog_record*)handle);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1int(
    JNIEnv *env, jclass obj, long handle, jbyteArray buf) {
    jlong res;
    if (buf != NULL) {
        jboolean isCopy;
        jbyte* b = (*env)->GetByteArrayElements(env, buf, &isCopy);
        jsize capacity = (*env)->GetArrayLength(env, buf);
        res = (jlong)ddlog_get_int((ddlog_record*)handle, (unsigned char*)b, capacity);
        (*env)->ReleaseByteArrayElements(env, buf, b, 0);
    } else {
        res = (jlong)ddlog_get_int((ddlog_record*)handle, NULL, 0);
    }
    return res;
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1i64(
    JNIEnv *env, jclass obj, long handle) {
    return (jlong)ddlog_get_i64((ddlog_record*)handle);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDlogAPI_ddlog_1is_1string(
    JNIEnv *env, jclass obj, long handle) {
    return (jboolean)ddlog_is_string((ddlog_record*)handle);
}

const jstring toJString(JNIEnv* env, const char* nonNullStr, size_t size) {
    char* buf = malloc(size + 1);
    if (buf == NULL) {
        return NULL;
    }
    strncpy(buf, nonNullStr, size);
    buf[size] = 0;
    jstring result = (*env)->NewStringUTF(env, buf);
    free(buf);
    return result;
}

JNIEXPORT jstring JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1str(
    JNIEnv *env, jclass obj, long handle) {
    const char *s = ddlog_get_str_non_nul((const ddlog_record*)handle);
    size_t size = ddlog_get_strlen((const ddlog_record*)handle);
    return toJString(env, s, size);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDlogAPI_ddlog_1is_1tuple(
    JNIEnv *env, jclass obj, long handle) {
    return (jboolean)ddlog_is_tuple((ddlog_record*)handle);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1tuple_1size(
    JNIEnv *env, jclass obj, long handle) {
    return (jint)ddlog_get_tuple_size((ddlog_record*)handle);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1tuple_1field(
    JNIEnv *env, jclass obj, jlong handle, jint index) {
    return (jlong)ddlog_get_tuple_field((ddlog_record*)handle, (size_t)index);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDlogAPI_ddlog_1is_1vector(
    JNIEnv *env, jclass obj, long handle) {
    return (jboolean)ddlog_is_vector((ddlog_record*)handle);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1vector_1size(
    JNIEnv *env, jclass obj, long handle) {
    return (jint)ddlog_get_vector_size((ddlog_record*)handle);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1vector_1elem(
    JNIEnv *env, jclass obj, jlong handle, jint index) {
    return (jlong)ddlog_get_vector_elem((ddlog_record*)handle, (size_t)index);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDlogAPI_ddlog_1is_1set(
    JNIEnv *env, jclass obj, long handle) {
    return (jboolean)ddlog_is_set((ddlog_record*)handle);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1set_1size(
    JNIEnv *env, jclass obj, long handle) {
    return (jint)ddlog_get_set_size((ddlog_record*)handle);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1set_1elem(
    JNIEnv *env, jclass obj, jlong handle, jint index) {
    return (jlong)ddlog_get_set_elem((ddlog_record*)handle, (size_t)index);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDlogAPI_ddlog_1is_1map(
    JNIEnv *env, jclass obj, long handle) {
    return (jboolean)ddlog_is_map((ddlog_record*)handle);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1map_1size(
    JNIEnv *env, jclass obj, long handle) {
    return (jint)ddlog_get_map_size((ddlog_record*)handle);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1map_1key(
    JNIEnv *env, jclass obj, jlong handle, jint index) {
    return (jlong)ddlog_get_map_key((ddlog_record*)handle, (size_t)index);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1map_1val(
    JNIEnv *env, jclass obj, jlong handle, jint index) {
    return (jlong)ddlog_get_map_val((ddlog_record*)handle, (size_t)index);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDlogAPI_ddlog_1is_1struct(
    JNIEnv *env, jclass obj, jlong handle) {
    return (jboolean)ddlog_is_struct((ddlog_record*)handle);
}

JNIEXPORT jstring JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1constructor(
    JNIEnv *env, jclass obj, jlong handle) {
    size_t size;
    const char *s = ddlog_get_constructor_non_null((const ddlog_record*)handle, &size);
    return toJString(env, s, size);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1get_1struct_1field(
    JNIEnv *env, jclass obj, jlong handle, jint index) {
    return (jlong)ddlog_get_struct_field((ddlog_record*)handle, (size_t)index);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1insert_1cmd(
    JNIEnv *env, jclass obj, jint table, jlong handle) {
    ddlog_cmd* result = ddlog_insert_cmd(table, (ddlog_record*)handle);
    return (jlong)result;
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1delete_1val_1cmd(
    JNIEnv *env, jclass obj, jint table, jlong handle) {
    ddlog_cmd* result = ddlog_delete_val_cmd(table, (ddlog_record*)handle);
    return (jlong)result;
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDlogAPI_ddlog_1delete_1key_1cmd(
    JNIEnv *env, jclass obj, jint table, jlong handle) {
    ddlog_cmd* result = ddlog_delete_key_cmd(table, (ddlog_record*)handle);
    return (jlong)result;
}
