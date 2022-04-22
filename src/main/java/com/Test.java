package com;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zhupeiwen
 * @date 2022/4/14
 */
public class Test {
    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        List<Integer> list1 = list.stream().filter(x -> !x.equals(1)).collect(Collectors.toList());

        for (Integer integer : list1) {
            System.out.println(integer);
        }
    }
}
