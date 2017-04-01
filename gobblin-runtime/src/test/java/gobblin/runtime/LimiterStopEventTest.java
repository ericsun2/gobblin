package gobblin.runtime;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import gobblin.configuration.ConfigurationKeys;
import gobblin.source.extractor.limiter.LimiterConfigurationKeys;
import gobblin.util.limiter.Limiter;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;
import junit.framework.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.mockito.Mockito.mock;


@Test(groups = {"gobblin.runtime"})
public class LimiterStopEventTest {
    @Test
    public void testGetLimiterStopMetadataCase0() throws InterruptedException {
        Properties properties = new Properties();
        String key1 = "topic";
        String key2 = "partition.id";
        String key3 = "others";
        String keyList = Joiner.on(',').join(key1, key2);
        properties.setProperty(LimiterConfigurationKeys.LIMITER_REPORT_KEY_LIST, keyList);
        properties.setProperty(key1, "1111");
        properties.setProperty(key2, "1111");

        Extractor extractor = mock (Extractor.class);
        Limiter limiter = mock (Limiter.class);
        TaskState taskState = mock (TaskState.class);
        WorkUnit workUnit =  mock (WorkUnit.class);
        Mockito.when(taskState.getWorkunit()).thenReturn(workUnit);
        Mockito.when(taskState.getJobId()).thenReturn("123");
        Mockito.when(taskState.getTaskAttemptId()).thenReturn(Optional.of("555"));
        Mockito.when(taskState.getTaskId()).thenReturn("888");
        Mockito.when(limiter.acquirePermits(1)).thenReturn(null);
        Mockito.when (taskState.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN)).thenReturn("file://xyz");
        Mockito.when(workUnit.getProperties()).thenReturn(properties);
        LimitingExtractorDecorator<String, String> decorator = new LimitingExtractorDecorator<>(extractor, limiter, taskState);
        try {
            Method method = LimitingExtractorDecorator.class.getDeclaredMethod("getLimiterStopMetadata");
            method.setAccessible(true);
            ImmutableMap<String, String> metaData = (ImmutableMap<String, String>)method.invoke(decorator);
            Assert.assertEquals(metaData.containsKey(key1), true);
            Assert.assertEquals(metaData.containsKey(key2), true);
            Assert.assertEquals(metaData.containsKey(key3), false);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetLimiterStopMetadataCase1() throws InterruptedException {
        Properties properties = new Properties();
        String key1 = "topic";
        String key2 = "partition.id";
        String keyList = Joiner.on(',').join(key1, key2);
        String subKey1 = key2 + ".0";
        String subKey2 = key2 + ".1";
        String subKey3 = key2 + ".2";
        String subKey4 = key2 + ".3";
        String subKey5 = "partition";
        properties.setProperty(LimiterConfigurationKeys.LIMITER_REPORT_KEY_LIST, keyList);
        properties.setProperty(subKey1, "1111");
        properties.setProperty(subKey2, "1111");
        properties.setProperty(subKey3, "1111");
        properties.setProperty(subKey4, "1111");

        Extractor extractor = mock (Extractor.class);
        Limiter limiter = mock (Limiter.class);
        TaskState taskState = mock (TaskState.class);
        WorkUnit workUnit =  mock (WorkUnit.class);
        Mockito.when(taskState.getWorkunit()).thenReturn(workUnit);
        Mockito.when(taskState.getJobId()).thenReturn("123");
        Mockito.when(taskState.getTaskAttemptId()).thenReturn(Optional.of("555"));
        Mockito.when(taskState.getTaskId()).thenReturn("888");
        Mockito.when(limiter.acquirePermits(1)).thenReturn(null);
        Mockito.when (taskState.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN)).thenReturn("file://xyz");
        Mockito.when(workUnit.getProperties()).thenReturn(properties);
        LimitingExtractorDecorator<String, String> decorator = new LimitingExtractorDecorator<>(extractor, limiter, taskState);
        try {
            Method method = LimitingExtractorDecorator.class.getDeclaredMethod("getLimiterStopMetadata");
            method.setAccessible(true);
            ImmutableMap<String, String> metaData = (ImmutableMap<String, String>)method.invoke(decorator);
            Assert.assertEquals(metaData.containsKey(subKey1), true);
            Assert.assertEquals(metaData.containsKey(subKey2), true);
            Assert.assertEquals(metaData.containsKey(subKey3), true);
            Assert.assertEquals(metaData.containsKey(subKey4), true);
            Assert.assertEquals(metaData.containsKey(subKey5), false);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetLimiterStopMetadataCase3() throws InterruptedException {
        Properties properties = new Properties();
        String key1 = "topic";
        String key2 = "partition.id";
        String keyList = Joiner.on(',').join(key1, key2);
        String subKey1 = key2 + "....";
        String subKey2 = key2 + "##fjpaierbng;";
        String subKey3 = key2 + "x[n  sdf";
        String subKey4 = key2 + "";
        properties.setProperty(LimiterConfigurationKeys.LIMITER_REPORT_KEY_LIST, keyList);
        properties.setProperty(subKey1, "1111");
        properties.setProperty(subKey2, "1111");
        properties.setProperty(subKey3, "1111");
        properties.setProperty(subKey4, "1111");
        properties.setProperty(key1,  "1111");
        properties.setProperty(key2,  "1111");

        Extractor extractor = mock (Extractor.class);
        Limiter limiter = mock (Limiter.class);
        TaskState taskState = mock (TaskState.class);
        WorkUnit workUnit =  mock (WorkUnit.class);
        Mockito.when(taskState.getWorkunit()).thenReturn(workUnit);
        Mockito.when(taskState.getJobId()).thenReturn("123");
        Mockito.when(taskState.getTaskAttemptId()).thenReturn(Optional.of("555"));
        Mockito.when(taskState.getTaskId()).thenReturn("888");
        Mockito.when(limiter.acquirePermits(1)).thenReturn(null);
        Mockito.when (taskState.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN)).thenReturn("file://xyz");
        Mockito.when(workUnit.getProperties()).thenReturn(properties);
        LimitingExtractorDecorator<String, String> decorator = new LimitingExtractorDecorator<>(extractor, limiter, taskState);
        try {
            Method method = LimitingExtractorDecorator.class.getDeclaredMethod("getLimiterStopMetadata");
            method.setAccessible(true);
            ImmutableMap<String, String> metaData = (ImmutableMap<String, String>)method.invoke(decorator);
            Assert.assertEquals(metaData.containsKey(key1), true);
            Assert.assertEquals(metaData.containsKey(key2), true);
            Assert.assertEquals(metaData.containsKey(subKey1), true);
            Assert.assertEquals(metaData.containsKey(subKey2), true);
            Assert.assertEquals(metaData.containsKey(subKey3), true);
            Assert.assertEquals(metaData.containsKey(subKey4), true);
        } catch (Exception e) {
            Assert.fail();
        }
    }
}
