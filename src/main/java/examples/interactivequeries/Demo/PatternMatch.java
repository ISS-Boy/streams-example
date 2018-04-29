package examples.interactivequeries.Demo;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import net.seninp.jmotif.sax.NumerosityReductionStrategy;
import net.seninp.jmotif.sax.SAXException;
import net.seninp.jmotif.sax.SAXProcessor;
import net.seninp.jmotif.sax.alphabet.NormalAlphabet;
import net.seninp.jmotif.sax.datastructure.SAXRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.mhealth.open.data.avro.MEvent;
import org.mhealth.open.data.avro.MPattern;
import org.mhealth.open.data.avro.Measure;
import org.mhealth.open.data.avro.patternResult;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class PatternMatch {

    private List<SymbolicPattern> symbolicPatterns;     //key是measure名字，value是模式字符串
    private String patternid;               //模式有可能是中文，所以改成模式的编号
    private SAXAnalysisWindow window;

    //alt+insert是构造器和get、set方法的快捷键，点第一个然后按shift点最后一个


    public PatternMatch(List<SymbolicPattern> symbolicPatterns, String patternid, SAXAnalysisWindow window) {
        this.symbolicPatterns = symbolicPatterns;
        this.patternid = patternid;
        this.window = window;
    }

    public List<SymbolicPattern> getSymbolicPatterns() {
        return symbolicPatterns;
    }

    public void setSymbolicPatterns(List<SymbolicPattern> symbolicPatterns) {
        this.symbolicPatterns = symbolicPatterns;
    }

    public String getPatternid() {
        return patternid;
    }

    public void setPatternid(String patternid) {
        this.patternid = patternid;
    }

    public SAXAnalysisWindow getWindow() {
        return window;
    }

    public void setWindow(SAXAnalysisWindow window) {
        this.window = window;
    }

    public void runKStream() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "pattern_match");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ParaConfig.bootstrapServers);
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ParaConfig.schemaRegistryUrl);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final SpecificAvroSerde<MEvent> mEventSerde = new SpecificAvroSerde<>();
        mEventSerde.configure(ParaConfig.serdeConfig, false);

        final SpecificAvroSerde<MPattern> mPatternSerde = new SpecificAvroSerde<>();
        mPatternSerde.configure(ParaConfig.serdeConfig, false);


        List<String> users = new ArrayList<>();             //用户数是从前台传进来的
        KStreamBuilder builder = new KStreamBuilder();
        Set<String> mk = new HashSet<>(symbolicPatterns.get(0).getMeasures().keySet());
        List<String> measures = new ArrayList<>(mk);        //measures集合里存的是维度
        KStream<String, MEvent> kStream = builder.stream(Serdes.String(), mEventSerde, judgeTopic(measures.get(0)));

/**
 * join是基于key的，如果只是按user_id进行join，结果是笛卡尔积，是多对多的关系，所以在 join 之前需要先 map 成
 * user_id + timestamp，成一对一的关系
  */
        if (mk.contains("systolic_blood_pressure") && mk.contains("diastolic_blood_pressure")) {
            kStream = builder.stream(Serdes.String(), mEventSerde, ParaConfig.TOPIC1);
            kStream
                    .filter((key, value) -> {
                        boolean flag = false;
                        for (String user : users) {
                            flag |= key.equals(user);
                        }
                        return flag;
                    })
                    .mapValues(value -> {
                        Measure m1 = new Measure();
                        m1.put("unit", "mmHg");
                        m1.put("value", value.getMeasures().get("systolic_blood_pressure").getValue());
                        Measure m2 = new Measure();
                        m2.put("unit", "mmHg");
                        m2.put("value", value.getMeasures().get("diastolic_blood_pressure").getValue());
                        MEvent m = new MEvent();
                        m.put("user_id", value.getUserId());
                        m.put("timestamp", value.getTimestamp());
                        m.put("measures", m1);
                        m.put("measures", m2);
                        return m;
                    });
            mk.remove("systolic_blood_pressure");
            mk.remove("diastolic_blood_pressure");

            measures = new ArrayList<>(mk);
            if (measures.size() > 0) {
                for (String measure : measures) {
                    KStream<String, MEvent> tempKStream = builder.stream(Serdes.String(), mEventSerde, judgeTopic(measure));
                    kStream = kStream
                            .filter((key, value) -> {
                                boolean flag = false;
                                for (String user : users) {
                                    flag |= key.equals(user);
                                }
                                return flag;
                            })
                            .map((key, value) -> KeyValue.pair(value.getUserId() + value.getTimestamp(), value))
                            .join(tempKStream
                                            .filter((key, value) -> {
                                                boolean flag = false;
                                                for (String user : users) {
                                                    flag |= key.equals(user);
                                                }
                                                return flag;
                                            })
                                            .map((key, value) -> KeyValue.pair(value.getUserId() + value.getTimestamp(), value)),
                                    (MEvent leftValue, MEvent rightValue) ->
                                    {
                                        MEvent mEvent = new MEvent();
                                        mEvent.put("user_id", leftValue.getUserId());
                                        mEvent.put("timestamp", leftValue.getTimestamp());
                                        for (String mkey : leftValue.getMeasures().keySet()) {
                                            Measure m = new Measure();
                                            m.put("unit", judgeUnit(mkey));
                                            m.put("value", rightValue.getMeasures().get(mkey).getValue());
                                            mEvent.put("measures", m);
                                        }
                                        Measure m2 = new Measure();
                                        m2.put("unit", judgeUnit(measure));
                                        m2.put("value", rightValue.getMeasures().get(measure).getValue());
                                        mEvent.put("measures", m2);
                                        return mEvent;
                                    },
                                    JoinWindows.of(TimeUnit.MINUTES.toMillis(10)));//指定时间窗口，在指定的时间窗口内会等待相同的key进行匹配
                }
            }
        } else {
            if (measures.size() > 0) {
                for (String measure : measures) {
                    KStream<String, MEvent> tempKStream = builder.stream(Serdes.String(), mEventSerde, judgeTopic(measure));
                    kStream = kStream
                            .filter((key, value) -> {
                                boolean flag = false;
                                for (String user : users) {
                                    flag |= key.equals(user);
                                }
                                return flag;
                            })
//join是基于key的，如果只是按user_id进行join，结果是笛卡尔积，是多对多的关系，所以需要先map成id+timestamp，成一对一的关系
                            .map((key, value) -> KeyValue.pair(value.getUserId() + value.getTimestamp(), value))
                            .join(tempKStream
                                            .filter((key, value) -> {
                                                boolean flag = false;
                                                for (String user : users) {
                                                    flag |= key.equals(user);
                                                }
                                                return flag;
                                            })
                                            .map((key, value) -> KeyValue.pair(value.getUserId() + value.getTimestamp(), value)),
                                    (MEvent leftValue, MEvent rightValue) ->
                                    {
                                        MEvent mEvent = new MEvent();
                                        mEvent.put("user_id", leftValue.getUserId());
                                        mEvent.put("timestamp", leftValue.getTimestamp());
                                        for (String mkey : leftValue.getMeasures().keySet()) {
                                            Measure m = new Measure();
                                            m.put("unit", judgeUnit(mkey));
                                            m.put("value", rightValue.getMeasures().get(mkey).getValue());
                                            mEvent.put("measures", m);
                                        }
                                        Measure m2 = new Measure();
                                        m2.put("unit", judgeUnit(measure));
                                        m2.put("value", rightValue.getMeasures().get(measure).getValue());
                                        mEvent.put("measures", m2);
                                        return mEvent;
                                    },
                                    JoinWindows.of(TimeUnit.MINUTES.toMillis(10)));//指定时间窗口，在指定的时间窗口内会等待相同的key进行匹配
                }
            }

        }


        int length = 66;

        List<Float> List1 = new ArrayList<>();
        List<Float> List2 = new ArrayList<>();

/**
 *  最外层的Map的key-value是<user_id,模式的集合>，list的长度就是模式的种数
 *  中间的Map的key-value是<measure，存储数据的集合>，list的长度就是length
 */
        Map<String, List<Map<String, List<Float>>>> map = new HashMap<>();
        for(String user : users){

        }

        Set<String> set = symbolicPatterns.get(0).getMeasures().keySet();
        List<String> mList = new ArrayList<>(set);      //维度的集合

        KStream<String, MPattern> kStream1  = kStream
                .map((key, value) -> {
                    MPattern mPattern = new MPattern();
                    for (int i = 0; i < map.get(key).size(); i++) {                     //模式的种数
                        for (int j = 0; j < map.get(key).get(i).size(); j++) {          //measures的种数
                            List<Float> list = map.get(key).get(i).get(mList.get(j));
                            double[] tsRed = new double[length];
                            while (list.size() < length - 1) {
                                list.add(value.getMeasures().get(mList.get(j)).getValue());
                            }
                            if (list.size() >= length) {
                                list.remove(0);
                                list.add(value.getMeasures().get(mList.get(j)).getValue());
                            } else
                                list.add(value.getMeasures().get(mList.get(j)).getValue());

                            for (int m = 0; m < length; m++)
                                tsRed[m] = list.get(m);


                            //将一维序列通过SAX算法转换，返回SAX记录值
                            SAXAnalysisWindow window = new SAXAnalysisWindow(64, 8, 4);
                            int slidingWindowSize = window.getnLength();
                            int paaSize = window.getwSegment();
                            int alphabetSize = window.getaAlphabet();
                            //NONE 是所有，没有省略
                            NumerosityReductionStrategy nrStrategy = NumerosityReductionStrategy.NONE;
                            int nThreshold = 1;

                            NormalAlphabet na = new NormalAlphabet();
                            SAXProcessor sp = new SAXProcessor();

                            SAXRecords res = null;
                            try {
                                res = sp.ts2saxViaWindow(tsRed, slidingWindowSize, paaSize,
                                        na.getCuts(alphabetSize), nrStrategy, nThreshold);
                            } catch (SAXException e) {
                                e.printStackTrace();
                            }

                            String alphabet = new String();
                            Set<Integer> index = res.getIndexes();
                            for (Integer idx : index)
                                alphabet += String.valueOf(res.getByIndex(idx).getPayload());
                            String s = "bbbbccccbbbbccccbbbbcccc";
                            boolean match = false;
                            try {
                                match = patternMatch(alphabet, s, alphabetSize);
                            } catch (SAXException e) {
                                e.printStackTrace();
                            }


                            mPattern.put("user_id", value.getUserId());
                            mPattern.put("timestamp", value.getTimestamp());

                            patternResult result = new patternResult();

                            if (match == true) {
                                list.removeAll(list);
                                result.put("i",0);
                                mPattern.put("measures",result);
                            } else {
                                result.put("i", 1);
                                mPattern.put("measures",result);
                            }
                        }
                    }
                    return KeyValue.pair(key, mPattern);
                });



//                .map((key, value) -> {
//                    while(List1.size()<length-1){
//                        List1.add(value.getMeasures().get("systolic_blood_pressure").getValue());
//                    }
//                    if(List1.size() >= length) {
//                        List1.remove(0);
//                        List1.add(value.getMeasures().get("systolic_blood_pressure").getValue());
//                    }
//                    else
//                        List1.add(value.getMeasures().get("systolic_blood_pressure").getValue());
//                    double[] tsRed = new double[length];
//                    for(int i=0;i<length;i++)
//                        tsRed[i] = List1.get(i);
//
//                    //将一维序列通过SAX算法转换，返回SAX记录值
//                    SAXAnalysisWindow window = new SAXAnalysisWindow(64,8,4);
//                    int slidingWindowSize = window.getnLength();
//                    int paaSize = window.getwSegment();
//                    int alphabetSize = window.getaAlphabet();
//                    //NONE 是所有，没有省略
//                    NumerosityReductionStrategy nrStrategy = NumerosityReductionStrategy.NONE;
//                    int nThreshold = 1;
//
//                    NormalAlphabet na = new NormalAlphabet();
//                    SAXProcessor sp = new SAXProcessor();
//
//                    SAXRecords res = null;
//                    try {
//                        res = sp.ts2saxViaWindow(tsRed, slidingWindowSize, paaSize,
//                                na.getCuts(alphabetSize), nrStrategy, nThreshold);
//                    } catch (SAXException e) {
//                        e.printStackTrace();
//                    }
//
//                    String alphabet = new String();
//                    Set<Integer> index = res.getIndexes();
//                    for (Integer idx : index)
//                        alphabet += String.valueOf(res.getByIndex(idx).getPayload());
//                    String s = "bbbbccccbbbbccccbbbbcccc";
//                    boolean match = false;
//                    try {
//                        match = patternMatch(alphabet,s,alphabetSize);
//                    } catch (SAXException e) {
//                        e.printStackTrace();
//                    }
//                    if(match == true) {
//                        List1.removeAll(List1);
//                        return KeyValue.pair(value.getUserId(), "1");
//                    } else
//                        return KeyValue.pair(value.getUserId(), "0");
//                    //返回值是0或1，不能用int型，因为特殊格式转int会报错
//                });


        //结果写入到一个topic中
        //key-value的形式，key是user_id，value是avro的格式，avro里有个map，map存放pattern和对应的pattern的0或者1
        //value也可以是string的形式，直接是模式id+0或1，然后opentsdb那边再解析

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().
                addShutdownHook(new Thread(streams::close));
    }

    private String judgeTopic(String measure) {
        String result = "";
        if (measure.equals("systolic_blood_pressure") || measure.equals("diastolic_blood_pressure")) {
            result = ParaConfig.TOPIC1;
        } else if (measure.equals("heart_rate")) {
            result = ParaConfig.TOPIC2;
        } else if (measure.equals("body_temperature")) {
            result = ParaConfig.TOPIC3;
        } else if (measure.equals("step_count")) {
            result = ParaConfig.TOPIC4;
        }
        return result;
    }

    private String judgeUnit(String measure) {
        String result = "";
        if (measure.equals("systolic_blood_pressure") || measure.equals("diastolic_blood_pressure")) {
            result = "mmHg";
        } else if (measure.equals("heart_rate")) {
            result = "beats/min";
        } else if (measure.equals("body_temperature")) {
            result = "C";
        } else if (measure.equals("step_count")) {
            result = "";
        }
        return result;
    }

    static boolean patternMatch(String alphabet1, String alphabet2, int length) throws SAXException {
        NormalAlphabet nal = new NormalAlphabet();
        double[][] distance = nal.getDistanceMatrix(length);
        for (int i = 0; i < alphabet1.length(); i++) {
            if (distance[alphabet1.charAt(i) - 'a'][alphabet2.charAt(i) - 'a'] != 0)
                return false;
        }
        return true;
    }
}
