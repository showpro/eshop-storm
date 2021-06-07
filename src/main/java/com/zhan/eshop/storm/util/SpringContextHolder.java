
package com.zhan.eshop.storm.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.Objects;


@Component
public class SpringContextHolder implements ApplicationContextAware {
    private static ApplicationContext applicationContext = null;

    public static <T> T getBeanByName(String beanName) {
        if (applicationContext == null) {
            return null;
        }
        return (T) applicationContext.getBean(beanName);
    }

    public static <T> T getBean(String name, Class<T> clazz) {
        return getApplicationContext().getBean(name, clazz);
    }

    public static <T> T getBean(Class<T> clazz) {
        return getApplicationContext().getBean(clazz);
    }

    public static Object getBean(String name) {
        return getApplicationContext().getBean(name);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SpringContextHolder.applicationContext = applicationContext;
    }

    private static ApplicationContext getApplicationContext() {
        if (Objects.isNull(applicationContext)) {
            throw new RuntimeException("applicationContext注入失败");
        }
        return applicationContext;
    }
}
