// simulates click to populate all text
document.getElementsByClassName('show-more-less-html__button show-more-less-html__button--more')[0].click()

// fetch text description
let text_data = document.getElementsByClassName('description')[0].innerText

function download(text, name, type) {

  var a = document.getElementsByTagName('a')[0]; // get proxy link element
  var file = new Blob([text], {type: type}); // create file blob with content
  a.href = URL.createObjectURL(file); // add object URL via proxy link
  a.download = name; // add download attribute to set file name
  a.click() // simulate click
}

download(text_data, 'job_description', 'txt')


function pausecomp(millis)
{
    var date = new Date();
    var curDate = null;
    do { curDate = new Date(); }
    while(curDate-date < millis);
}

for (index in document.getElementsByClassName('result-card__full-card-link')){
document.getElementsByClassName('result-card__full-card-link')[index].click()
}
