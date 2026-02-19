package com.bdq.api.reference;

import com.bdq.api.cache.CacheNames;
import com.bdq.api.cache.ExternalResponseCache;
import com.bdq.api.reference.model.CountryGeometry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.index.strtree.STRtree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class PointCountryResolver {

    private static final Logger log = LoggerFactory.getLogger(PointCountryResolver.class);
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory(new PrecisionModel(), 4326);

    private final ReferenceDataManager dataManager;
    private final ReferenceDataProperties properties;
    private final ObjectMapper objectMapper;
    private final ExternalResponseCache cache;

    private final AtomicReference<STRtree> indexRef = new AtomicReference<>();
    private final AtomicReference<List<CountryGeometry>> geometries = new AtomicReference<>(List.of());

    public PointCountryResolver(ReferenceDataManager dataManager,
                                ReferenceDataProperties properties,
                                ObjectMapper objectMapper,
                                ExternalResponseCache cache) {
        this.dataManager = dataManager;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.cache = cache;
    }

    public Optional<String> resolveCountry(double latitude, double longitude) {
        String cacheKey = gridKey(latitude, longitude, properties.getSpatial().getGridSizeDegrees());
        String iso = cache.compute(CacheNames.POINT_TO_COUNTRY, cacheKey,
                () -> computeCountry(latitude, longitude));
        if (iso == null || iso.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(iso);
    }

    public void rebuildIndex() {
        loadGeometries();
    }

    private String computeCountry(double latitude, double longitude) {
        ensureIndexLoaded();
        STRtree index = indexRef.get();
        if (index == null) {
            return null;
        }
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude));
        @SuppressWarnings("unchecked")
        List<CountryGeometry> candidates = index.query(point.getEnvelopeInternal());
        for (CountryGeometry cg : candidates) {
            Geometry geometry = cg.getGeometry();
            if (geometry != null && (geometry.contains(point) || geometry.covers(point))) {
                return cg.getIsoA2();
            }
        }
        return null;
    }

    private void ensureIndexLoaded() {
        if (indexRef.get() == null) {
            synchronized (this) {
                if (indexRef.get() == null) {
                    loadGeometries();
                }
            }
        }
    }

    private void loadGeometries() {
        try {
            Path path = dataManager.ensureDataset("natural-earth-admin0");
            JsonNode root = objectMapper.readTree(path.toFile());
            JsonNode features = root.get("features");
            if (features == null || !features.isArray()) {
                log.warn("No feature collection found in {}", path);
                return;
            }
            List<CountryGeometry> loaded = new ArrayList<>();
            for (JsonNode feature : features) {
                JsonNode propertiesNode = feature.get("properties");
                String isoA2 = null;
                if (propertiesNode != null) {
                    if (propertiesNode.has("iso_a2")) {
                        isoA2 = propertiesNode.get("iso_a2").asText();
                    } else if (propertiesNode.has("ISO3166-1-Alpha-2")) {
                        isoA2 = propertiesNode.get("ISO3166-1-Alpha-2").asText();
                    } else if (propertiesNode.has("ISO_A2")) {
                        isoA2 = propertiesNode.get("ISO_A2").asText();
                    }
                }
                String name = propertiesNode != null && propertiesNode.has("name")
                        ? propertiesNode.get("name").asText()
                        : null;
                JsonNode geometryNode = feature.get("geometry");
                if (geometryNode == null || geometryNode.isNull()) {
                    continue;
                }
                Geometry geometry = parseGeometry(geometryNode);
                if (geometry == null) continue;
                loaded.add(new CountryGeometry(isoA2, name, geometry));
            }
            STRtree tree = new STRtree();
            for (CountryGeometry cg : loaded) {
                tree.insert(cg.getGeometry().getEnvelopeInternal(), cg);
            }
            tree.build();
            geometries.set(List.copyOf(loaded));
            indexRef.set(tree);
            log.info("Loaded {} country polygons into STRtree index", loaded.size());
        } catch (IOException e) {
            log.error("Failed to load country boundaries", e);
        }
    }

    private Geometry parseGeometry(JsonNode node) {
        String type = node.get("type").asText();
        return switch (type) {
            case "Polygon" -> buildPolygon(node.get("coordinates"));
            case "MultiPolygon" -> buildMultiPolygon(node.get("coordinates"));
            default -> null;
        };
    }

    private Polygon buildPolygon(JsonNode coordinateNode) {
        if (coordinateNode == null || !coordinateNode.isArray() || coordinateNode.isEmpty()) {
            return null;
        }
        LinearRing shell = buildLinearRing(coordinateNode.get(0));
        if (shell == null) {
            return null;
        }
        return GEOMETRY_FACTORY.createPolygon(shell);
    }

    private Geometry buildMultiPolygon(JsonNode coordinateNode) {
        if (coordinateNode == null || !coordinateNode.isArray()) {
            return null;
        }
        List<Polygon> polygons = new ArrayList<>();
        for (JsonNode polygonNode : coordinateNode) {
            Polygon polygon = buildPolygon(polygonNode);
            if (polygon != null) {
                polygons.add(polygon);
            }
        }
        if (polygons.isEmpty()) {
            return null;
        }
        return GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(Polygon[]::new));
    }

    private LinearRing buildLinearRing(JsonNode coordinateNode) {
        if (coordinateNode == null || !coordinateNode.isArray()) {
            return null;
        }
        List<Coordinate> coordinates = new ArrayList<>();
        for (JsonNode coord : coordinateNode) {
            if (!coord.isArray() || coord.size() < 2) continue;
            coordinates.add(new Coordinate(coord.get(0).asDouble(), coord.get(1).asDouble()));
        }
        if (coordinates.isEmpty()) {
            return null;
        }
        Coordinate first = coordinates.get(0);
        Coordinate last = coordinates.get(coordinates.size() - 1);
        if (!first.equals2D(last)) {
            coordinates.add(first);
        }
        return GEOMETRY_FACTORY.createLinearRing(coordinates.toArray(Coordinate[]::new));
    }

    private String gridKey(double latitude, double longitude, double gridSize) {
        double snappedLat = Math.floor(latitude / gridSize) * gridSize;
        double snappedLon = Math.floor(longitude / gridSize) * gridSize;
        return String.format(Locale.ROOT, "%.3f:%.3f", snappedLat, snappedLon);
    }
}
