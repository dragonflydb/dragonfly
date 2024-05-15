const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');
const UglifyJS = require('uglify-js');
const CleanCSS = require('clean-css');
const { minify } = require('html-minifier-terser');

// Function to read a file
const readFile = (filePath) => {
    return fs.readFileSync(path.join('src', filePath), 'utf8');
};

// Function to convert an image to Base64
const encodeImageToBase64 = (imagePath) => {
    const extname = path.extname(imagePath).substring(1);
    const base64 = fs.readFileSync(path.join('src', imagePath), 'base64');
    return `data:image/${extname};base64,${base64}`;
};

// Function to inline CSS
const inlineCSS = (cssPaths) => {
    return cssPaths.map((cssPath) => {
        const cssContent = readFile(cssPath);
        return new CleanCSS().minify(cssContent).styles;
    }).join(' ');
};

// Function to inline JS
const inlineJS = (jsPaths) => {
    return jsPaths.map((jsPath) => {
        const jsContent = readFile(jsPath);
        return UglifyJS.minify(jsContent).code;
    }).join('; ');
};

// Main function to inline all assets into HTML
const inlineAssets = async (htmlPath) => {
    let htmlContent = readFile(htmlPath);
    const $ = cheerio.load(htmlContent);

    // Collect all CSS paths
    const cssPaths = [];
    $('link[rel="stylesheet"]').each((_, element) => {
        cssPaths.push($(element).attr('href'));
    });

    // Collect all JS paths
    const jsPaths = [];
    $('script[src]').each((_, element) => {
        jsPaths.push($(element).attr('src'));
    });

    // Collect all image paths
    const imagePaths = [];
    $('img').each((_, element) => {
        imagePaths.push($(element).attr('src'));
    });

    // Inline CSS
    const inlinedCSS = inlineCSS(cssPaths);
    $('link[rel="stylesheet"]').remove();
    $('head').append(`<style>${inlinedCSS}</style>`);

    // Inline JS
    const inlinedJS = inlineJS(jsPaths);
    $('script[src]').remove();
    $('body').append(`<script>${inlinedJS}</script>`);

    // Inline images
    imagePaths.forEach((imagePath) => {
        const base64Image = encodeImageToBase64(imagePath);
        $(`img[src="${imagePath}"]`).attr('src', base64Image);
    });

    // Get the final HTML content
    htmlContent = $.html();

    // Minify HTML
    const minifiedHTML = await minify(htmlContent, {
        collapseWhitespace: true,
        removeComments: true,
        minifyCSS: true,
        minifyJS: true,
    });

    // Write the inlined HTML to a new file

    if (!fs.existsSync('build')) fs.mkdirSync('build');
    fs.writeFileSync('build/index.html', minifiedHTML, 'utf8');
    console.log('Inlined HTML created: build/index.html');
};

// Path to your HTML file
const htmlPath = 'index.html';

// Run the inline process
inlineAssets(htmlPath);
