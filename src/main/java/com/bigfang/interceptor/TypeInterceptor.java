package com.bigfang.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName TypeInterceptor
 * @Description TODO
 * @Author yaoyong.fang
 * @Date 2019/9/6 15:34
 * @Version 1.0
 **/
public class TypeInterceptor implements Interceptor {
    private List<Event> addHeaderEvents;
    
    @Override
    public void initialize() {
        addHeaderEvents = new ArrayList<>();
    }
    
    //单个事件拦截
    @Override
    public Event intercept(Event event) {
        //获取事件的头部信息
        final Map<String, String> headers = event.getHeaders();
        //获取body中的信息
        final String body = new String(event.getBody());
        //根据body中的内容添加event中的头部信息
        if(body.contains("test")){
            headers.put("type", "test");
        }else{
            headers.put("type", "others");
        }
        return event;
    }
    
    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event:list) {
            final Event addHeaderEvent = intercept(event);
            addHeaderEvents.add(addHeaderEvent);
        }
        return addHeaderEvents;
    }
    
    @Override
    public void close() {
    
    }
    
    public static class Builder implements Interceptor.Builder{
    
        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }
    
        @Override
        public void configure(Context context) {
            
        }
    }
}
