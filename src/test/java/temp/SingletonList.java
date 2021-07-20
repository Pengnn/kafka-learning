package temp;

import java.util.Collections;
import java.util.List;

/**
 * @Description TODO
 * @Author Pengnan
 * @CreateTime 2021年07月18日 20:16:00
 */
public class SingletonList {
    public static void main(String[] args) {
        int[] nums={1,2,3};
        List<Object> list = Collections.singletonList(nums);
        System.out.println(list);
    }
}
