/* Implementation of external functions called from Java */

#include <jni.h>
#include <stdio.h>
#include "external_External.h"

JNIEXPORT void JNICALL
Java_external_External_print(JNIEnv *env, jobject obj) {
    printf("Hello World!\n");
    return;
}
