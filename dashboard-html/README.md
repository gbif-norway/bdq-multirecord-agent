# Biodiversity Data Quality Report

A clean, modular HTML dashboard for visualizing biodiversity data quality test results.

## Structure

The report has been refactored into a clean, maintainable structure:

### Files

- `bdq-report.html` - Main HTML file (simplified)
- `styles.css` - All CSS styles
- `app.js` - Main application entry point
- `js/config.js` - Configuration constants
- `js/utils.js` - Utility functions
- `js/data-structures.js` - Data management
- `js/data-processing.js` - CSV parsing and data processing
- `js/charts.js` - Chart building logic
- `js/ui-rendering.js` - UI rendering functions
- `js/url-handling.js` - URL parameter handling
- `js/error-handling.js` - Error handling utilities

### Key Improvements

1. **Modular Architecture**: Code is split into logical modules with clear responsibilities
2. **DRY Principle**: Eliminated duplicate code and consolidated similar functions
3. **Configuration**: All constants and settings centralized in `config.js`
4. **Error Handling**: Comprehensive error handling with user-friendly messages
5. **Maintainability**: Easy to modify and extend individual components

### Configuration

Edit `js/config.js` to customize:
- UI dimensions and limits
- Chart colors and styling
- Error messages
- External library URLs
- Performance settings

### Usage

The report loads automatically when opened in a browser. It accepts URL parameters:
- `?results=filename.csv` - Results CSV file
- `?original=filename.csv` - Original data file (optional)

### Dependencies

- Chart.js (loaded from CDN)
- PapaParse (loaded from CDN)
- Tabulator (loaded from CDN)
- Font Awesome (loaded from CDN)

### Browser Support

Requires modern browsers with ES6 module support.
