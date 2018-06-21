package com.evun.xutil.springmvc;

import cn.evun.gap.eps.util.JacksonUtils;
import com.fasterxml.jackson.databind.JavaType;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.MethodParameter;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.support.WebArgumentResolver;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.servlet.mvc.annotation.AnnotationMethodHandlerAdapter;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Created by wq on 8/26/17.
 * JsonString注解处理器, 用在controller中的方法参数中
 */
@SuppressWarnings("Since15")
public class JsonStringHandler implements WebArgumentResolver, ApplicationContextAware {

    @Override
    public Object resolveArgument(MethodParameter methodParameter, NativeWebRequest nativeWebRequest) throws Exception {
        //如果参数不包含JsonString注解, 直接返回, 不处理
        if (!methodParameter.hasParameterAnnotation(JsonString.class)) {
            return UNRESOLVED;
        }
        JsonString annotation = methodParameter.getParameterAnnotation(JsonString.class);
        //获取注解中的值, 比如 @JsonString("jsonStr") MaterialDTO material, 则获取到 jsonStr
        String annoValue = annotation.value();
        String jsonString = null;
        if (StringUtils.isEmpty(annoValue)) {
            //不允许控制
            throw new RuntimeException("JsonString annotation needs non-empty value!");
        } else {
            //获取前台传来的json字符串
            jsonString = nativeWebRequest.getParameter(annoValue);
        }
        //获取参数类型
        Class<?> parameterType = methodParameter.getParameterType();
        //如果参数是List类型, 则获取泛型的具体类型, 下例 List<Bean>
        if (parameterType.isAssignableFrom(List.class)) {
            Type type = ((ParameterizedTypeImpl) methodParameter.getGenericParameterType()).getActualTypeArguments()[0];
            //type在例子中类型为Bean
            JavaType javaType = JacksonUtils.mapper.getTypeFactory().constructParametricType(List.class, (Class<?>) type);
            //jackson支持通过JavaType指定的复合类型进行反序列化
            return JacksonUtils.mapper.readValue(jsonString, javaType);
        }
        //如果参数不是List, 则当做普通Bean类型进行解析
        //不支持其他集合类型, 比如Set, Map, [](数组)
        return JacksonUtils.toObject(jsonString, parameterType);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        AnnotationMethodHandlerAdapter handlerAdapter = applicationContext.getBean(AnnotationMethodHandlerAdapter.class);
        //将自定义参数解析器设置到当前的 AnnotationMethodHandlerAdapter中
        handlerAdapter.setCustomArgumentResolver(this);
    }
}
