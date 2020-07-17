package com.barry.flink.domain;

import lombok.Data;

/**
 * @ClassName UserBehavior
 * @Author wangxuexing
 * @Date 2020/7/11 21:56
 * @Version 1.0
 */
@Data
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;
}
