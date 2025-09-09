import { showToast } from './utils.js';

// Error handling utilities
export class AppError extends Error {
  constructor(message, code = 'UNKNOWN_ERROR', details = null) {
    super(message);
    this.name = 'AppError';
    this.code = code;
    this.details = details;
  }
}

export function handleError(error, context = '') {
  console.error(`Error in ${context}:`, error);
  
  let userMessage = 'An unexpected error occurred.';
  let toastDuration = 4000;
  
  if (error instanceof AppError) {
    userMessage = error.message;
    toastDuration = 3000;
  } else if (error.name === 'TypeError') {
    userMessage = 'A configuration or data format error occurred.';
  } else if (error.name === 'NetworkError' || error.message.includes('fetch')) {
    userMessage = 'Network error - please check your connection and try again.';
  } else if (error.message.includes('CSV') || error.message.includes('parse')) {
    userMessage = 'Error parsing data file. Please check the file format.';
  }
  
  showToast(`${context ? context + ': ' : ''}${userMessage}`, { ms: toastDuration });
  
  // Log additional details for debugging
  if (error.details) {
    console.error('Error details:', error.details);
  }
}

export async function withErrorHandling(fn, context = '') {
  try {
    return await fn();
  } catch (error) {
    handleError(error, context);
    throw error;
  }
}

export function validateUrl(url, paramName = 'URL') {
  if (!url) {
    throw new AppError(`No ${paramName} provided`, 'MISSING_URL');
  }
  
  try {
    new URL(url);
    return true;
  } catch {
    throw new AppError(`Invalid ${paramName} format: ${url}`, 'INVALID_URL', { url });
  }
}

export function validateDataStructure(data, expectedFields = []) {
  if (!data || typeof data !== 'object') {
    throw new AppError('Invalid data structure received', 'INVALID_DATA_STRUCTURE');
  }
  
  for (const field of expectedFields) {
    if (!(field in data)) {
      throw new AppError(`Missing required field: ${field}`, 'MISSING_FIELD', { field, data });
    }
  }
  
  return true;
}
