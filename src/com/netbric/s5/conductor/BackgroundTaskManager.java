package com.netbric.s5.conductor;

import com.netbric.s5.conductor.rpc.RestfulReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Hashtable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BackgroundTaskManager {
    static final Logger logger = LoggerFactory.getLogger(BackgroundTaskManager.class);
    static BackgroundTaskManager instance = new BackgroundTaskManager();
    static public BackgroundTaskManager getInstance() {
        return instance;
    }


    private long idSeed;
    public Hashtable<Long, BackgroundTask> taskMap;
    private ExecutorService threadPool = Executors.newFixedThreadPool(1);

    public BackgroundTask initiateTask(TaskType type, String desc, TaskExecutor exe, Object arg){
        BackgroundTask t = new BackgroundTask();
        t.id = ++idSeed;
        t.type = type;
        t.desc = desc;
        t.startTime = new Date();
        t.arg = arg;
        t.exec = exe;
        t.status = TaskStatus.WAITING;

        taskMap.put(t.id, t);
        threadPool.submit(() -> {
            t.status = TaskStatus.RUNNING;
            try {
                t.exec.run(t);
                t.finishTime = new Date();
                t.status =TaskStatus.SUCCEEDED;
            } catch (Exception e) {
                t.finishTime = new Date();
                logger.error("Backend task failed,id:{} type:{} desc:{}, startTime:{}, reason:{}", t.id, t.type, t.desc, t.startTime, e);
                t.status =  TaskStatus.FAILED;
            }

        });
        return t;
    }


    static public enum TaskType {RECOVERY, SCRUB, GC}
    static public enum TaskStatus{WAITING, RUNNING, SUCCEEDED, FAILED }
    static public interface TaskExecutor {
        void run(BackgroundTask arg) throws Exception;
    }
    static public class BackgroundTask {
        public long id;
        public TaskType type;
        public String desc;
        public TaskStatus status;
        public int progress;
        public java.util.Date startTime;
        public java.util.Date finishTime;
        RestfulReply result;

        Object arg;
        TaskExecutor exec;
    }
}
