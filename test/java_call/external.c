/* Implementation of external functions called from Java */

#include <jni.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "external_External.h"

JNIEXPORT void JNICALL
Java_external_External_hello(JNIEnv *env, jobject obj) {
    printf("Hello World!\n");
    return;
}

JNIEXPORT jstring JNICALL
Java_external_External_concat(JNIEnv *env, jobject obj, jstring left, jstring right) {
    const char* l = (*env)->GetStringUTFChars(env, left, NULL);
    const char* r = (*env)->GetStringUTFChars(env, right, NULL);
    int len = strlen(l) + strlen(r) + 1;
    char* buf = malloc(len);
    if (buf == NULL)
        return NULL;
    strncpy(buf, l, len);
    strncat(buf, r, len);
    jstring result = (*env)->NewStringUTF(env, buf);
    free(buf);
    return result;
}
