package com.tutorial.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * <p>author     : zhupeiwen
 * <p>date       : 2021/11/23 10:47 上午
 * <p>description:
 */
public class SortTest {
    public static void main(String[] args) {
        List<Integer> list = Arrays.asList(1, 3, 5, 6, 2, 4, 2);
        ArrayList<Integer> arrayList = new ArrayList<>(list);
        arrayList.sort(new MyComparator2());
        for (Integer integer : arrayList) {
            System.out.print(integer);
        }
    }

    static class MyComparator1 implements Comparator<Integer> {
        @Override
        public int compare(Integer a, Integer b) {  //源码中第一个入参（a）是数组靠后面的数，第二个入参（b）是数组靠前面的数（比如这里：a=3，b=1）
            if (a <= b) {    //由条件加上返回值来确定是升序还是降序 （如果全部返回-1的话，则实现逆序，将集合中的元素顺序颠倒）
                return 1;   //比如这里：原数组后面a的数小于前面的数b，返回1，1则表示这个顺序不需要调整。
            } else {
                return -1;  //比如这里：原数组后面的数a小于前面的数b，返回-1,-1则表示数组中现在的顺序需要调整。根据我们的代码，前两个元素是1和3，判断条件if(3<=1)，返回的是-1，即不满足我们的期望。 下面排序的时候就会对顺序进行调整了。
            }
        }
    }

    static class MyComparator2 implements Comparator<Integer> {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1 - o2; // o1 后面的 o2前面的  【后面的大于前面的返回正，不需要调整】，【后面的小于前面的返回负，需要调整顺序】最终后面大于前面，即升序
        }
    }
}
