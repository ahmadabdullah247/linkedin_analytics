// simulates click to populate all text
document.getElementsByClassName('show-more-less-html__button show-more-less-html__button--more')[0].click()

function download(text, name, type) {

  var a = document.getElementsByTagName('a')[0]; // get proxy link element
  var file = new Blob([text], {type: type}); // create file blob with content
  a.href = URL.createObjectURL(file); // add object URL via proxy link
  a.download = name; // add download attribute to set file name
  a.click() // simulate click
}
download()
