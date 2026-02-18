package com.bdq.api.runner;

import com.bdq.api.model.TestInfo;
import org.datakurator.ffdq.annotations.*;
import org.springframework.stereotype.Component;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TestRegistry {

    public static class Entry {
        public final Method method;
        public final Class<?> declaringClass;
        public final TestInfo info;
        public Entry(Method method, Class<?> declaringClass, TestInfo info) {
            this.method = method;
            this.declaringClass = declaringClass;
            this.info = info;
        }
    }

    private final Map<String, Entry> byId = new ConcurrentHashMap<>();
    private final Map<String, Entry> byGuid = new ConcurrentHashMap<>();

    public TestRegistry() {
        // Known FP DQ classes to scan (static methods)
        List<String> classes = Arrays.asList(
                "org.filteredpush.qc.georeference.DwCGeoRefDQ",
                "org.filteredpush.qc.sciname.DwCSciNameDQ",
                "org.filteredpush.qc.sciname.DwCSciNameDQDefaults",
                "org.filteredpush.qc.date.DwCEventDQ",
                "org.filteredpush.qc.date.DwCOtherDateDQ",
                "org.filteredpush.qc.metadata.DwCMetadataDQ",
                "org.filteredpush.qc.metadata.DwCMetadataDQDefaults"
        );
        for (String cn : classes) {
            try {
                Class<?> c = Class.forName(cn);
                scanClass(c);
            } catch (Throwable ignore) {
                // class not on classpath or load error; skip
            }
        }
    }

    private void scanClass(Class<?> clazz) {
        for (Method m : clazz.getDeclaredMethods()) {
            Provides provides = m.getAnnotation(Provides.class);
            if (provides == null) continue;

            String type = "Unknown";
            if (m.isAnnotationPresent(Validation.class)) type = "Validation";
            else if (m.isAnnotationPresent(Amendment.class)) type = "Amendment";
            else if (m.isAnnotationPresent(Measure.class)) type = "Measure";
            else if (m.isAnnotationPresent(Issue.class)) type = "Issue";

            String id = null;
            if (m.isAnnotationPresent(Validation.class)) {
                id = m.getAnnotation(Validation.class).label();
            } else if (m.isAnnotationPresent(Amendment.class)) {
                id = m.getAnnotation(Amendment.class).label();
            } else if (m.isAnnotationPresent(Measure.class)) {
                id = m.getAnnotation(Measure.class).label();
            } else if (m.isAnnotationPresent(Issue.class)) {
                id = m.getAnnotation(Issue.class).label();
            }

            String guid = provides.value();

            List<String> actedUpon = new ArrayList<>();
            List<String> consulted = new ArrayList<>();
            List<String> parameters = new ArrayList<>();
            Annotation[][] paramAnns = m.getParameterAnnotations();
            for (Annotation[] annSet : paramAnns) {
                boolean matched = false;
                for (Annotation a : annSet) {
                    if (a instanceof ActedUpon) {
                        actedUpon.add(((ActedUpon) a).value());
                        matched = true;
                    } else if (a instanceof Consulted) {
                        consulted.add(((Consulted) a).value());
                        matched = true;
                    } else if (a instanceof Parameter) {
                        parameters.add(((Parameter) a).name());
                        matched = true;
                    }
                }
                if (!matched) {
                    parameters.add(""); // placeholder for unannotated param
                }
            }

            TestInfo info = new TestInfo();
            info.setId(id);
            info.setGuid(guid);
            info.setType(type);
            info.setClassName(clazz.getName());
            info.setMethodName(m.getName());
            info.setActedUpon(actedUpon);
            info.setConsulted(consulted);
            info.setParameters(parameters);

            Entry e = new Entry(m, clazz, info);

            // Prefer Defaults classes over base classes when duplicates occur
            boolean isDefaults = clazz.getSimpleName().endsWith("Defaults");

            if (id != null && !id.isEmpty()) {
                byId.merge(id, e, (oldV, newV) -> prefer(oldV, newV));
            }
            if (guid != null && !guid.isEmpty()) {
                byGuid.merge(guid, e, (oldV, newV) -> prefer(oldV, newV));
            }
        }
    }

    private Entry prefer(Entry a, Entry b) {
        boolean aDefaults = a.declaringClass.getSimpleName().endsWith("Defaults");
        boolean bDefaults = b.declaringClass.getSimpleName().endsWith("Defaults");
        if (aDefaults && !bDefaults) return a;
        if (bDefaults && !aDefaults) return b;
        // Tie-breaker: fewer parameters (defaults typically wrap with fewer)
        int aParams = a.method.getParameterCount();
        int bParams = b.method.getParameterCount();
        if (aParams != bParams) return (aParams < bParams) ? a : b;
        // Stable preference: keep first seen
        return a;
    }

    public List<TestInfo> list() {
        // Deduplicate by GUID primarily, then ID
        Map<String, TestInfo> byGuidOnly = new LinkedHashMap<>();
        for (Entry e : byGuid.values()) {
            if (e.info.getGuid() != null) byGuidOnly.putIfAbsent(e.info.getGuid(), e.info);
        }
        // Add any entries that lack GUID but have ID
        for (Entry e : byId.values()) {
            if (e.info.getGuid() == null || e.info.getGuid().isEmpty()) {
                String id = e.info.getId();
                if (id != null && !id.isEmpty() && !containsId(byGuidOnly.values(), id)) {
                    byGuidOnly.put("id:" + id, e.info);
                }
            }
        }
        List<TestInfo> out = new ArrayList<>(byGuidOnly.values());
        out.sort(Comparator.comparing(t -> Optional.ofNullable(t.getId()).orElse("")));
        return out;
    }

    private boolean containsId(Collection<TestInfo> values, String id) {
        for (TestInfo t : values) {
            if (id.equals(t.getId())) return true;
        }
        return false;
    }

    public Optional<Entry> find(String idOrGuid) {
        if (idOrGuid == null) return Optional.empty();
        Entry e = byId.get(idOrGuid);
        if (e != null) return Optional.of(e);
        e = byGuid.get(idOrGuid);
        return Optional.ofNullable(e);
    }
}
