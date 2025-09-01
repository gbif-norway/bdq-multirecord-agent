# LLM Integration Deployment Guide

This guide explains how to deploy the new LLM-powered email summaries for the BDQ Email Report Service.

## Overview

The service now uses Google Gemini to generate intelligent, contextual email summaries that:
- Analyze the user's original email context
- Provide data quality scores and insights
- Explain issues in plain language for biologists
- Offer actionable advice for improving data quality
- Maintain a helpful, encouraging tone

## Google Cloud Run Changes

### 1. Environment Variables

Add the following environment variable to your Google Cloud Run service:

```bash
GOOGLE_API_KEY=your_google_api_key_here
```

**To set this in Google Cloud Console:**
1. Go to Cloud Run in the Google Cloud Console
2. Select your `bdq-multirecord-agent` service
3. Click "EDIT & DEPLOY NEW REVISION"
4. In the "Variables & Secrets" section, add:
   - **Variable name**: `GOOGLE_API_KEY`
   - **Value**: Your Google API key

### 2. Get a Google API Key

1. Visit [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Sign in with your Google account
3. Click "Create API Key"
4. Copy the generated key
5. Add it to your Cloud Run environment variables

### 3. API Quotas and Limits

- **Free tier**: 15 requests per minute, 1500 requests per day
- **Paid tier**: $0.50 per 1M characters input, $1.50 per 1M characters output
- **Rate limits**: 60 requests per minute for paid accounts

For typical usage (3 emails per week), the free tier should be sufficient.

## Testing Locally

### 1. Set up environment

```bash
# Create .env file
echo "GOOGLE_API_KEY=your_api_key_here" > .env

# Install dependencies
pip install -r requirements.txt
```

### 2. Test the LLM service

```bash
python test_llm.py
```

This will test the LLM integration with sample data.

### 3. Test with Docker

```bash
# Build the image
docker build -t bdq-multirecord-agent .

# Run with environment variable
docker run -p 8080:8080 -e GOOGLE_API_KEY=your_api_key_here bdq-multirecord-agent
```

## How It Works

### 1. Email Processing Flow

1. **Email received** → CSV processed → BDQ tests run
2. **LLM Context Preparation** → Analyzes test results, user email, dataset info
3. **Gemini API Call** → Generates intelligent summary
4. **Email Reply** → Sends LLM-generated summary + attachments

### 2. LLM Prompt Structure

The LLM receives:
- Dataset type (Occurrence/Taxon core)
- Test results and validation failures
- User's original email context
- Data quality score calculation
- Amendment insights

### 3. Fallback Behavior

If the LLM service is unavailable:
- Falls back to basic summary generation
- No interruption to email processing
- Logs warnings for monitoring

## Monitoring and Debugging

### 1. Logs to Watch

```bash
# Check Cloud Run logs
gcloud logs read --service=bdq-multirecord-agent --limit=50

# Look for LLM-related messages:
# - "LLM service disabled" → API key missing
# - "Error generating LLM summary" → API issues
# - "Falling back to basic summary" → LLM unavailable
```

### 2. Discord Notifications

The service sends Discord notifications for:
- Processing errors
- LLM fallbacks
- General status updates

### 3. Health Check

The `/health` endpoint shows:
- Service status
- Environment variable configuration
- Service health

## Cost Optimization

### 1. Free Tier Usage

- Monitor your usage at [Google AI Studio](https://makersuite.google.com/app/usage)
- Free tier: 15 requests/minute, 1500 requests/day
- Typical usage: ~3 emails/week = ~12 LLM calls/week

### 2. Prompt Optimization

The current prompt is optimized for:
- Clear, actionable advice
- Biologist-friendly language
- Consistent formatting
- Reasonable token usage

### 3. Caching (Future Enhancement)

Consider implementing result caching to avoid re-running LLM calls for similar datasets.

## Troubleshooting

### Common Issues

1. **"LLM service disabled"**
   - Check `GOOGLE_API_KEY` environment variable
   - Verify API key is valid

2. **"Error generating LLM summary"**
   - Check API quotas and limits
   - Verify network connectivity from Cloud Run
   - Check API key permissions

3. **Fallback to basic summary**
   - LLM service unavailable
   - Check logs for specific error messages

### Support

- **Google AI Studio**: [makersuite.google.com](https://makersuite.google.com)
- **Cloud Run Logs**: Use `gcloud logs` command
- **Discord**: Check webhook notifications

## Security Considerations

1. **API Key Security**
   - Never commit API keys to version control
   - Use environment variables in Cloud Run
   - Rotate keys periodically

2. **Data Privacy**
   - LLM prompts include user email content
   - No data is stored by Google beyond the API call
   - Consider data sensitivity in prompts

3. **Rate Limiting**
   - Monitor API usage
   - Implement backoff for failed requests
   - Graceful degradation to basic summaries
