const checkInterval = 1000;

function checkIndexing() {
    const xhttp = new XMLHttpRequest();
    xhttp.onload = function() {
        const flash = document.getElementById("flash");
        if (this.responseText === "YES") {
            flash.classList.remove("hidden");
            return;
        } else if (this.responseText === "NO") {
            if (flash.classList.contains("hidden")) {
                flash.classList.add("hidden");
            }
        } else {
            alert("There was an error trying to resume");
        }

        // keep checking until indexing has finished
        setTimeout(checkIndexing, interval);
    };
    xhttp.open("GET", "/indexing", true);
    xhttp.send();
}

//setTimeout(checkIndexing, checkInterval);