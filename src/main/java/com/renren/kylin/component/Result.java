package com.renren.kylin.component;

import java.io.Serializable;

/**
 * @author : peng.chen5@renren-inc.com
 * @Time : 2017/7/20 下午12:23
 */
public class Result implements Serializable {
    private static final long serialVersionUID = 2893936715547157050L;

    private transient static final int SUCCESS = 200;
    private transient static final int FAIL = 100;

    private int result;
    private String message;
    private Object data;

    public Result() {
    }

    public static Result success(){
        Result result = new Result();
        result.setResult(SUCCESS);
        return result;
    }

    public static Result fail(String message){
        Result result = new Result();
        result.setResult(FAIL);
        result.setMessage(message);
        return result;
    }

    public static Result success(Object data){
        Result result = new Result();
        result.setResult(SUCCESS);
        result.setData(data);
        return result;
    }

    public boolean isSuccess(){
        return result == SUCCESS;
    }


    public int getResult() {
        return result;
    }

    public void setResult(int result) {
        this.result = result;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "Result{" +
                "result=" + result +
                ", message='" + message + '\'' +
                ", data=" + data +
                '}';
    }
}
