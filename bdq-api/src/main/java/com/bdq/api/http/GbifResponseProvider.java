package com.bdq.api.http;

import com.bdq.api.reference.TaxonomyBridge;
import com.bdq.api.reference.model.GbifBackboneEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.filteredpush.qc.sciname.services.GBIFService;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class GbifResponseProvider implements OfflineResponseCache.OfflineResponseProvider {

    private final AtomicBoolean initialised = new AtomicBoolean(false);
    private final AtomicReference<Map<String, List<Entry>>> byCanonical = new AtomicReference<>(Map.of());
    private final AtomicReference<Map<Integer, Entry>> byKey = new AtomicReference<>(Map.of());

    @Override
    public Optional<OfflineResponseCache.ResponseContent> resolve(URI uri, String method, Map<String, List<String>> headers) throws IOException {
        if (!"GET".equalsIgnoreCase(method)) {
            return Optional.empty();
        }
        if (!"api.gbif.org".equalsIgnoreCase(uri.getHost())) {
            return Optional.empty();
        }
        if (TaxonomyBridge.mapper().isEmpty()) {
            return Optional.empty();
        }

        ensureLoaded();
        if (byCanonical.get().isEmpty()) {
            return Optional.empty();
        }

        String path = uri.getPath();
        if ("/v1/species".equals(path) || "/v1/species/".equals(path) || "/v1/species/search".equals(path)) {
            return Optional.ofNullable(handleSearch(uri));
        }
        if (path.startsWith("/v1/species/") && path.length() > "/v1/species/".length()) {
            String remainder = path.substring("/v1/species/".length());
            if (!remainder.isBlank() && !"search".equals(remainder)) {
                return Optional.ofNullable(handleFetchById(remainder));
            }
        }
        return Optional.empty();
    }

    private OfflineResponseCache.ResponseContent handleSearch(URI uri) throws IOException {
        Map<String, List<String>> params = parseQuery(uri.getQuery());
        String datasetKey = first(params.get("datasetKey"));
        if (datasetKey != null && !datasetKey.isBlank() && !GBIFService.KEY_GBIFBACKBONE.equals(datasetKey)) {
            return emptyResponse(parseInt(first(params.get("limit")), 100));
        }
        String rank = Optional.ofNullable(first(params.get("rank"))).map(r -> r.toUpperCase(Locale.ROOT)).orElse("");
        String name = Optional.ofNullable(first(params.get("name"))).orElseGet(() -> first(params.get("q")));
        if (name == null || name.isBlank()) {
            return emptyResponse(parseInt(first(params.get("limit")), 100));
        }
        int limit = parseInt(first(params.get("limit")), 100);
        List<Map<String, Object>> results;
        if ("GENUS".equals(rank)) {
            results = buildGenusResults(name, limit);
        } else {
            results = buildSpeciesResults(name, limit);
        }
        return buildResponse(results, limit);
    }

    private OfflineResponseCache.ResponseContent handleFetchById(String idPart) throws IOException {
        int idx = idPart.indexOf('/');
        String token = idx >= 0 ? idPart.substring(0, idx) : idPart;
        int key;
        try {
            key = Integer.parseInt(token);
        } catch (NumberFormatException ex) {
            return null;
        }
        Entry entry = byKey.get().get(key);
        if (entry == null) {
            return emptyObject();
        }
        Map<String, Object> node = buildSpeciesNode(entry);
        return jsonResponse(node);
    }

    private void ensureLoaded() {
        if (initialised.get()) {
            return;
        }
        synchronized (this) {
            if (initialised.get()) {
                return;
            }
            List<GbifBackboneEntry> entries = TaxonomyBridge.allEntries();
            if (entries.isEmpty()) {
                return;
            }
            Map<String, List<Entry>> canonicalIndex = new LinkedHashMap<>();
            Map<Integer, Entry> keyIndex = new LinkedHashMap<>();
            for (GbifBackboneEntry entry : entries) {
                if (entry == null || entry.canonicalName() == null) {
                    continue;
                }
                Entry wrapped = new Entry(generateId(entry.canonicalName()), entry);
                canonicalIndex.computeIfAbsent(entry.canonicalAuthorityKey(), k -> new ArrayList<>()).add(wrapped);
                keyIndex.put(wrapped.key(), wrapped);
            }
            byCanonical.set(canonicalIndex);
            byKey.set(keyIndex);
            initialised.set(true);
        }
    }

    private OfflineResponseCache.ResponseContent buildResponse(List<Map<String, Object>> results, int limit) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("offset", 0);
        body.put("limit", limit);
        body.put("endOfRecords", Boolean.TRUE);
        body.put("count", results.size());
        body.put("results", results);
        return jsonResponse(body);
    }

    private OfflineResponseCache.ResponseContent emptyResponse(int limit) throws IOException {
        return buildResponse(List.of(), limit);
    }

    private OfflineResponseCache.ResponseContent emptyObject() throws IOException {
        return jsonResponse(Map.of());
    }

    private OfflineResponseCache.ResponseContent jsonResponse(Object payload) throws IOException {
        ObjectMapper mapper = TaxonomyBridge.mapper().orElseThrow();
        byte[] bytes = mapper.writeValueAsBytes(payload);
        Map<String, List<String>> headers = Map.of("Content-Type", List.of("application/json"));
        return new OfflineResponseCache.ResponseContent(headers, bytes);
    }

    private List<Map<String, Object>> buildSpeciesResults(String name, int limit) {
        String canonical = canonicalise(name);
        List<Entry> entries = new ArrayList<>(lookupByCanonical(canonical));
        if (entries.isEmpty()) {
            String lower = canonical.toLowerCase(Locale.ROOT);
            for (Map.Entry<String, List<Entry>> e : byCanonical.get().entrySet()) {
                if (e.getKey().contains(lower)) {
                    entries.addAll(e.getValue());
                }
            }
        }
        List<Map<String, Object>> results = new ArrayList<>();
        for (Entry entry : entries) {
            results.add(buildSpeciesNode(entry));
            if (results.size() >= limit) {
                break;
            }
        }
        return results;
    }

    private List<Map<String, Object>> buildGenusResults(String genusName, int limit) {
        String target = genusName.trim().toLowerCase(Locale.ROOT);
        Set<String> seen = new LinkedHashSet<>();
        List<Map<String, Object>> results = new ArrayList<>();
        for (List<Entry> entries : byCanonical.get().values()) {
            for (Entry entry : entries) {
                String genus = Optional.ofNullable(entry.entry().genus()).orElse("").toLowerCase(Locale.ROOT);
                if (genus.equals(target) && seen.add(genus)) {
                    results.add(buildGenusNode(entry));
                }
            }
            if (results.size() >= limit) {
                break;
            }
        }
        return results;
    }

    private Map<String, Object> buildSpeciesNode(Entry entry) {
        Map<String, Object> node = new LinkedHashMap<>();
        GbifBackboneEntry data = entry.entry();
        String canonical = data.canonicalName();
        node.put("key", entry.key());
        node.put("usageKey", entry.key());
        node.put("datasetKey", GBIFService.KEY_GBIFBACKBONE);
        node.put("scientificName", canonical);
        node.put("canonicalName", canonical);
        node.put("taxonomicStatus", "ACCEPTED");
        node.put("rank", "SPECIES");
        node.put("kingdom", optional(data.kingdom()));
        node.put("phylum", optional(data.phylum()));
        if (data.clazz() != null) {
            node.put("class", data.clazz());
            node.put("clazz", data.clazz());
        }
        node.put("order", optional(data.order()));
        node.put("family", optional(data.family()));
        node.put("genus", optional(data.genus()));
        node.put("authorship", "");
        node.put("numDescendants", 0);
        node.put("synonym", Boolean.FALSE);
        node.put("taxonID", "local:" + entry.key());
        node.put("created", Instant.EPOCH.toString());
        return node;
    }

    private Map<String, Object> buildGenusNode(Entry entry) {
        Map<String, Object> node = new LinkedHashMap<>();
        GbifBackboneEntry data = entry.entry();
        String genus = Optional.ofNullable(data.genus()).orElse("");
        int key = generateId("genus:" + genus);
        node.put("key", key);
        node.put("usageKey", key);
        node.put("datasetKey", GBIFService.KEY_GBIFBACKBONE);
        node.put("scientificName", genus);
        node.put("canonicalName", genus);
        node.put("taxonomicStatus", "ACCEPTED");
        node.put("rank", "GENUS");
        node.put("kingdom", Optional.ofNullable(entry.entry().kingdom()).orElse(""));
        node.put("phylum", Optional.ofNullable(entry.entry().phylum()).orElse(""));
        if (entry.entry().clazz() != null) {
            node.put("class", entry.entry().clazz());
            node.put("clazz", entry.entry().clazz());
        }
        node.put("order", Optional.ofNullable(entry.entry().order()).orElse(""));
        node.put("family", Optional.ofNullable(entry.entry().family()).orElse(""));
        node.put("genus", genus);
        node.put("numDescendants", 0);
        node.put("synonym", Boolean.FALSE);
        node.put("taxonID", "local-genus:" + genus);
        node.put("created", Instant.EPOCH.toString());
        return node;
    }

    private List<Entry> lookupByCanonical(String canonical) {
        String key = canonical.toLowerCase(Locale.ROOT);
        return byCanonical.get().getOrDefault(key, List.of());
    }

    private static Map<String, List<String>> parseQuery(String query) {
        Map<String, List<String>> params = new LinkedHashMap<>();
        if (query == null || query.isBlank()) {
            return params;
        }
        for (String part : query.split("&")) {
            if (part.isBlank()) continue;
            int idx = part.indexOf('=');
            String key;
            String value;
            if (idx >= 0) {
                key = URLDecoder.decode(part.substring(0, idx), StandardCharsets.UTF_8);
                value = URLDecoder.decode(part.substring(idx + 1), StandardCharsets.UTF_8);
            } else {
                key = URLDecoder.decode(part, StandardCharsets.UTF_8);
                value = "";
            }
            params.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        }
        return params;
    }

    private static String first(List<String> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.get(0);
    }

    private static int parseInt(String value, int defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    private static String canonicalise(String name) {
        if (name == null) {
            return "";
        }
        return name.trim().replaceAll("\\s+", " ");
    }

    private static String optional(String value) {
        return value == null ? "" : value;
    }

    private static int generateId(String value) {
        return Math.abs(Objects.hash(value)) + 1000;
    }

    private record Entry(int key, GbifBackboneEntry entry) {}
}
