# Build Process Documentation

## JAR Build Strategy

### Overview
This project uses a **Docker-based build strategy** for the Java CLI JAR. The JAR is built during the Docker image build process, not committed to the repository.

### Why Docker Build?

✅ **Reproducible builds** - Same Maven/Java version every time  
✅ **Platform independent** - Works on any development machine  
✅ **Security** - No pre-built binaries in repository  
✅ **Clean repository** - No large binary files tracked by git  
✅ **CI/CD ready** - Build process is fully containerized  

### JAR Size (96MB)

The CLI JAR is large because it's a "fat JAR" containing all dependencies:
- FilteredPush BDQ libraries (geo_ref_qc, sci_name_qc, event_date_qc, rec_occur_qc)
- Transitive dependencies (Jackson, SLF4J, etc.)
- This is **expected and normal** for a self-contained CLI tool

### Build Process

#### Docker Build (Production)
```bash
# Build Docker image - JAR is built automatically
docker build -t bdq-service .

# JAR location in container: /opt/bdq/bdq-cli.jar
```

#### Local Development Build (Optional)
```bash
# Build JAR locally for testing
cd java
mvn clean package -DskipTests

# JAR output: bdq-cli/target/bdq-cli-1.0.0.jar
```

### Important Rules

❌ **NEVER commit JAR files** - They are ignored by .gitignore  
❌ **NEVER commit target/ directories** - Maven build artifacts are ignored  
✅ **Always build through Docker for production**  
✅ **Local builds are for development/testing only**  

### Docker Multi-Stage Build

The Dockerfile uses a multi-stage build:

1. **Build Stage** (`maven:3.9-eclipse-temurin-17`)
   - Copies Java source code
   - Runs `mvn clean package -DskipTests`
   - Produces the CLI JAR

2. **Runtime Stage** (`python:3.11-slim`)  
   - Installs OpenJDK 21 JRE
   - Copies only the CLI JAR from build stage
   - Copies Python application code
   - Sets up runtime environment

### File Structure

```
java/
├── bdq-cli/target/
│   ├── bdq-cli-1.0.0.jar          ← 96MB fat JAR (ignored by git)
│   └── original-bdq-cli-1.0.0.jar ← 68KB slim JAR (ignored by git)
├── geo_ref_qc/target/              ← Maven artifacts (ignored by git)
├── sci_name_qc/target/             ← Maven artifacts (ignored by git)
├── event_date_qc/target/           ← Maven artifacts (ignored by git)
└── rec_occur_qc/target/            ← Maven artifacts (ignored by git)
```

### Development Workflow

1. **Make changes** to Java or Python code
2. **Test locally** with `mvn clean package` if needed
3. **Build Docker image** to test full integration
4. **Commit source changes** only (JAR files are automatically ignored)
5. **Deploy via Docker** - JAR is built fresh each time

### Troubleshooting

**Large JAR size warning?** → Normal, expected to be ~96MB  
**JAR not found in container?** → Check Docker build logs for Maven errors  
**Local JAR vs Docker JAR differences?** → Always use Docker build for production  

## Clean Architecture Benefits

The new clean architecture also improves build efficiency:
- ✅ Removed 105+ lines of CSV parsing code from CLI
- ✅ CLI is now a simple executor (smaller, faster)
- ✅ Python handles all CSV parsing and test mapping
- ✅ Individual test execution with detailed timing