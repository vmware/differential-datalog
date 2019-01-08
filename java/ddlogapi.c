/* Wrapper that converts a JNI C interface to calls to the native ddlog C API */

#include <stdlib.h>
#include <string.h>
#include "ddlogapi_DDLogAPI.h"
#include "ddlog.h"

/* The _1 in all the function names below
   is the JNI translation of the _ character from Java */

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1run(
    JNIEnv *env, jobject obj, jint workers) {
    if (workers <= 0)
        workers = 1;
    return (jlong)ddlog_run((unsigned)workers, true, NULL, NULL);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDLogAPI_ddlog_1stop(
    JNIEnv *env, jobject obj, jlong handle) {
    return ddlog_stop((ddlog_prog)handle);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDLogAPI_ddlog_1transaction_1start(
    JNIEnv * env, jobject obj, jlong handle) {
    return ddlog_transaction_start((ddlog_prog)handle);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDLogAPI_ddlog_1transaction_1commit(
    JNIEnv * env, jobject obj, jlong handle) {
    return ddlog_transaction_commit((ddlog_prog)handle);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDLogAPI_ddlog_1transaction_1rollback(
    JNIEnv * env, jobject obj, jlong handle) {
    return ddlog_transaction_rollback((ddlog_prog)handle);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDLogAPI_ddlog_1apply_1updates(
    JNIEnv *env, jclass obj, jlong progHandle, jlongArray commandHandles) {
    jlong *a = (*env)->GetLongArrayElements(env, commandHandles, NULL);
    size_t size = (*env)->GetArrayLength(env, commandHandles);
    ddlog_cmd** updates = malloc(sizeof(ddlog_cmd*) * size);
    for (size_t i = 0; i < size; i++)
        updates[i] = (ddlog_cmd*)a[i];
    int result = ddlog_apply_updates(
        (ddlog_prog)progHandle, updates, size);
    (*env)->ReleaseLongArrayElements(env, commandHandles, a, 0);
    free(updates);
    return (jint)result;
}

struct CallbackInfo {
    JNIEnv* env;
    jobject obj;
    const char* method;
};

JNIEXPORT jint JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1table_1id(
    JNIEnv *env, jclass class, jstring table) {
    const char* tbl = (*env)->GetStringUTFChars(env, table, NULL);
    table_id id = ddlog_get_table_id(tbl);
    (*env)->ReleaseStringUTFChars(env, table, tbl);
    return (jint)id;
}

bool dump_callback(uintptr_t callbackInfo, const ddlog_record* rec) {
    struct CallbackInfo* cbi = (struct CallbackInfo*)callbackInfo;
    JNIEnv* env = cbi->env;
    jclass thisClass = (*env)->GetObjectClass(env, cbi->obj);
    jmethodID method = (*env)->GetMethodID(env, thisClass, cbi->method, "(J)Z");
    if (method == NULL)
        return false;
    jboolean result = (*env)->CallBooleanMethod(env, cbi->obj, method, (jlong)rec);
    return (bool)result;
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDLogAPI_dump_1table(
    JNIEnv *env, jobject obj, jlong progHandle, jint table, jstring callback) {
    const char* cbk = (*env)->GetStringUTFChars(env, callback, NULL);
    struct CallbackInfo* cbinfo = malloc(sizeof(struct CallbackInfo));
    if (cbinfo == NULL)
        return -1;
    cbinfo->env = env;
    cbinfo->obj = obj;
    cbinfo->method = cbk;
    int result = ddlog_dump_table((ddlog_prog)progHandle, table, dump_callback, (uintptr_t)cbinfo);
    free(cbinfo);
    (*env)->ReleaseStringUTFChars(env, callback, cbk);
    return (jint)result;
}

JNIEXPORT jstring JNICALL Java_ddlogapi_DDLogAPI_ddlog_1profile(
    JNIEnv *env, jobject obj, jlong progHandle) {
    char* profile = ddlog_profile((ddlog_prog)progHandle);
    jstring result = (*env)->NewStringUTF(env, profile);
    ddlog_string_free(profile);
    return result;
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1bool(
    JNIEnv *env, jclass obj, jboolean b) {
    return (jlong)ddlog_bool(b);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1u64(
    JNIEnv *env, jclass obj, jlong l) {
    return (jlong)ddlog_u64(l);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1string(
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
    for (size_t i = 0; i < len; i++)
        fields[i] = (ddlog_record*)a[i];
    ddlog_record* result = creator(fields, (size_t)len);
    (*env)->ReleaseLongArrayElements(env, handles, a, 0);
    free(fields);
    return (jlong)result;
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1tuple(
    JNIEnv *env, jclass obj, jlongArray handles) {
    return create_from_vector(env, obj, handles, ddlog_tuple);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1vector(
    JNIEnv *env, jclass obj, jlongArray handles) {
    return create_from_vector(env, obj, handles, ddlog_vector);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1set(
    JNIEnv *env, jclass obj, jlongArray handles) {
    return create_from_vector(env, obj, handles, ddlog_set);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1map(
    JNIEnv *env, jclass obj, jlongArray handles) {
    return create_from_vector(env, obj, handles, ddlog_map);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1pair(
    JNIEnv *env, jclass obj, jlong h1, jlong h2) {
    return (jlong)ddlog_pair((ddlog_record*)h1, (ddlog_record*)h2);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1struct(
    JNIEnv *env, jclass obj, jstring s, jlongArray handles) {
    const char* str = (*env)->GetStringUTFChars(env, s, NULL);
    jsize len = (*env)->GetArrayLength(env, handles);
    jlong *a = (*env)->GetLongArrayElements(env, handles, NULL);
    if (a == NULL)
        return -1;
    ddlog_record** fields = malloc(len * sizeof(ddlog_record*));
    for (size_t i = 0; i < len; i++)
        fields[i] = (ddlog_record*)a[i];
    ddlog_record* result = ddlog_struct(str, fields, len);
    (*env)->ReleaseLongArrayElements(env, handles, a, 0);
    free(fields);
    (*env)->ReleaseStringUTFChars(env, s, str);
    return (jlong)result;
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDLogAPI_ddlog_1is_1bool(
    JNIEnv *env, jclass obj, jlong handle) {
    return (jboolean)ddlog_is_bool((ddlog_record*)handle);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1bool(
    JNIEnv *env, jclass obj, jlong handle) {
    return (jboolean)ddlog_get_bool((ddlog_record*)handle);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDLogAPI_ddlog_1is_1int(
    JNIEnv *env, jclass obj, long handle) {
    return (jboolean)ddlog_is_int((ddlog_record*)handle);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1u64(
    JNIEnv *env, jclass obj, long handle) {
    return (jlong)ddlog_get_u64((ddlog_record*)handle);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDLogAPI_ddlog_1is_1string(
    JNIEnv *env, jclass obj, long handle) {
    return (jboolean)ddlog_is_string((ddlog_record*)handle);
}

const jstring toJString(JNIEnv* env, const char* nonNullStr, size_t size) {
    char* buf = malloc(size + 1);
    strncpy(buf, nonNullStr, size);
    buf[size] = 0;
    jstring result = (*env)->NewStringUTF(env, buf);
    free(buf);
    return result;
}

JNIEXPORT jstring JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1str(
    JNIEnv *env, jclass obj, long handle) {
    const char *s = ddlog_get_str_non_nul((const ddlog_record*)handle);
    size_t size = ddlog_get_strlen((const ddlog_record*)handle);
    return toJString(env, s, size);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDLogAPI_ddlog_1is_1tuple(
    JNIEnv *env, jclass obj, long handle) {
    return (jboolean)ddlog_is_tuple((ddlog_record*)handle);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1tuple_1size(
    JNIEnv *env, jclass obj, long handle) {
    return (jint)ddlog_get_tuple_size((ddlog_record*)handle);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1tuple_1field(
    JNIEnv *env, jclass obj, jlong handle, jint index) {
    return (jlong)ddlog_get_tuple_field((ddlog_record*)handle, (size_t)index);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDLogAPI_ddlog_1is_1vector(
    JNIEnv *env, jclass obj, long handle) {
    return (jboolean)ddlog_is_vector((ddlog_record*)handle);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1vector_1size(
    JNIEnv *env, jclass obj, long handle) {
    return (jint)ddlog_get_vector_size((ddlog_record*)handle);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1vector_1elem(
    JNIEnv *env, jclass obj, jlong handle, jint index) {
    return (jlong)ddlog_get_vector_elem((ddlog_record*)handle, (size_t)index);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDLogAPI_ddlog_1is_1set(
    JNIEnv *env, jclass obj, long handle) {
    return (jboolean)ddlog_is_set((ddlog_record*)handle);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1set_1size(
    JNIEnv *env, jclass obj, long handle) {
    return (jint)ddlog_get_set_size((ddlog_record*)handle);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1set_1elem(
    JNIEnv *env, jclass obj, jlong handle, jint index) {
    return (jlong)ddlog_get_set_elem((ddlog_record*)handle, (size_t)index);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDLogAPI_ddlog_1is_1map(
    JNIEnv *env, jclass obj, long handle) {
    return (jboolean)ddlog_is_map((ddlog_record*)handle);
}

JNIEXPORT jint JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1map_1size(
    JNIEnv *env, jclass obj, long handle) {
    return (jint)ddlog_get_map_size((ddlog_record*)handle);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1map_1key(
    JNIEnv *env, jclass obj, jlong handle, jint index) {
    return (jlong)ddlog_get_map_key((ddlog_record*)handle, (size_t)index);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1map_1val(
    JNIEnv *env, jclass obj, jlong handle, jint index) {
    return (jlong)ddlog_get_map_val((ddlog_record*)handle, (size_t)index);
}

JNIEXPORT jboolean JNICALL Java_ddlogapi_DDLogAPI_ddlog_1is_1struct(
    JNIEnv *env, jclass obj, jlong handle) {
    return (jboolean)ddlog_is_struct((ddlog_record*)handle);
}

JNIEXPORT jstring JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1constructor(
    JNIEnv *env, jclass obj, jlong handle) {
    size_t size;
    const char *s = ddlog_get_constructor_non_null((const ddlog_record*)handle, &size);
    return toJString(env, s, size);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1get_1struct_1field(
    JNIEnv *env, jclass obj, jlong handle, jint index) {
    return (jlong)ddlog_get_struct_field((ddlog_record*)handle, (size_t)index);
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1insert_1cmd(
    JNIEnv *env, jclass obj, jint table, jlong handle) {
    ddlog_cmd* result = ddlog_insert_cmd(table, (ddlog_record*)handle);
    return (jlong)result;
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1delete_1val_1cmd(
    JNIEnv *env, jclass obj, jint table, jlong handle) {
    ddlog_cmd* result = ddlog_delete_val_cmd(table, (ddlog_record*)handle);
    return (jlong)result;
}

JNIEXPORT jlong JNICALL Java_ddlogapi_DDLogAPI_ddlog_1delete_1key_1cmd(
    JNIEnv *env, jclass obj, jint table, jlong handle) {
    ddlog_cmd* result = ddlog_delete_key_cmd(table, (ddlog_record*)handle);
    return (jlong)result;
}
