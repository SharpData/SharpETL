package com.github.sharpdata.sharpetl.core.annotation;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface source {
    String[] types();
}
