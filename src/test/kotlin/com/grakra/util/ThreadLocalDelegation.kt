package com.grakra.util

import kotlin.properties.ReadWriteProperty
import kotlin.reflect.KProperty

fun <T> thread_local(): ReadWriteProperty<Any?, T?> =
        object : ReadWriteProperty<Any?, T?> {
            val data = ThreadLocal<T?>()
            override fun getValue(thisRef: Any?, property: KProperty<*>): T? = data.get()

            override fun setValue(thisRef: Any?, property: KProperty<*>, value: T?) {
                data.set(value)
            }

        }