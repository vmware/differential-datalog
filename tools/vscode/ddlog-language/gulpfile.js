const gulp  = require('gulp');
const merge = require('gulp-merge-json');
const json5 = require('gulp-json5-to-json');

gulp.task('default', (done) => {done() });

gulp.task('compile-json', (done ) => {
  gulp.src('./src/**/*.json5')
    .pipe(merge({
      fileName: "ddlog.tmLanguage.json",
      json5: true,
    }))
    .pipe(json5({
      beautify: true,
    }))
    .pipe(gulp.dest('./syntaxes'));
  done()
});

function done() {}
