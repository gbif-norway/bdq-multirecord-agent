# Development Rules

## **RULE #1: ALL LOCAL DEVELOPMENT MUST BE IN DOCKER**

**NO EXCEPTIONS**: All testing, development, and execution must happen inside Docker containers.

### What this means:
- ❌ **NEVER** run `python script.py` directly on the host
- ❌ **NEVER** install packages with `pip install` on the host  
- ❌ **NEVER** run Java/Maven commands directly on the host
- ❌ **NEVER** assume local Python/Java environments

### What to do instead:
- ✅ **ALWAYS** use `docker run` or `docker compose` 
- ✅ **ALWAYS** use the test containers for Python scripts
- ✅ **ALWAYS** mount files as volumes for testing
- ✅ **ALWAYS** use existing Docker images when possible

### Examples:

**❌ WRONG:**
```bash
python test_script.py
pip install requests
java -jar cli.jar
```

**✅ CORRECT:**
```bash
docker compose -f docker-compose.test.yml --profile test run --rm test-runner python test_script.py
docker compose -f docker-compose.test.yml --profile test run --rm -v ./test_script.py:/app/test_script.py test-runner python test_script.py
docker run --rm -p 8080:8080 bdq-multirecord-agent:fixed
```

## 🚫 **ENFORCEMENT**
If Claude tries to run anything locally outside Docker:
1. **STOP immediately** 
2. **Rewrite using Docker**
3. **Reference this file**

No local development. Docker only.

## **RULE 2: THIS IS A NEW PROJECT, DO NOT KEEP LEGACY CODE AND DO NOT PROVISION FOR BACKWARDS COMPATIBILITY! NO GRACEFUL DEGRADATION!**

Delete and clean up anything which is no longer relevant. We have the code history in git, we do not need to keep anything irrelevant. Do NOT make the app "degrade gracefully", fix things at their fundamentals.

## **RULE 3: DO NOT SUGGEST JUST RUNNING FEWER TESTS, ALL TESTS MUST BE RUN**

The entire point of this app is that it's supposed to run ALL relevant BDQ tests on any dataset which gets sent in. We don't just run the "top 10" tests or similar. We provide a comprehensive report. 

## **RULE 4: MINIMAL DOCUMENTATOIN**

If something is obvious, do not put it in the documentation, but ALL IMPORTANT DECISIONS MUST BE DOCUMENTED. By obvious, I mean something which is clear from just reading the code - don't document it. But if you have to read across multiple files or it's important conceptually to understand something, document it. 

When documenting a design decision, add it to one of the current md files, do not create a new md. Make sure you don't add fluff, don't write congratulatory epistles on how much more efficient or good something is, just state very simply what it is and how it works with the rest of the app, in the same style as the way the README is written. 

## **RULE 5: ADD MINIMAL CODE**

Do not add multiple safety fixes and checks in one go. Implement a fix, check if it works, if it doesn't then remove it and try something else. I don't want to have multiple attempts to e.g. start the bdq gateway service and not know which is the one which is actually doing the work. In this repo if you come across a problem you do ONE small fix, and make it as clean and minimal as possible. 
