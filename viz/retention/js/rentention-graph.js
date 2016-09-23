/**
 * Created by Araja Jyothi Babu on 22-May-16.
 */
/*
(function(factory) {
    'use strict';
    if (typeof define === 'function' && define.amd) {
        define(['jquery'], factory);
    } else if (typeof exports !== 'undefined') {
        module.exports = factory(require('jquery'));
    } else {
        factory(jQuery);
    }

}(function($) {
    'use strict';
    var Retention = window.Retention || {};

    Retention = (function() {

        function Retention(element, options) {

        }
        return Retention;

    }());

    $.fn.retention = function() {

    };

}));*/



function getRows(data){
    var rows = [];
    var keys = Object.keys(data);
    var days = [];
    var percentDays = [];
    for(var key in keys){
        if(data.hasOwnProperty(keys[key])) {
            days = data[keys[key]];
            percentDays.push(keys[key]);
            for(var i = 0; i < days.length; i++){
                percentDays.push(i > 0 ? Math.round((days[i]/days[0] * 100) * 100) / 100 : days[i]);
            }
            rows.push(percentDays);
            percentDays = [];
        }
    }
    return rows;
}

function isValidHex(color){
    return /(^#[0-9A-F]{6}$)|(^#[0-9A-F]{3}$)/i.test(color);
}

function shadeColor(color, percent) { //#
    color = isValidHex(color) ? color : "#3f83a3"; //handling null color;
    percent = 1.0 - Math.ceil(percent / 10) / 10;
    var f=parseInt(color.slice(1),16),t=percent<0?0:255,p=percent<0?percent*-1:percent,R=f>>16,G=f>>8&0x00FF,B=f&0x0000FF;
    return "#"+(0x1000000+(Math.round((t-R)*p)+R)*0x10000+(Math.round((t-G)*p)+G)*0x100+(Math.round((t-B)*p)+B)).toString(16).slice(1);
}

function tooltipData(count, dayIndex){
    return  (count + "% of users were active after " + (dayIndex - 1)) + (dayIndex == 2 ? "day" : " days");
}

var options = {
        data: {
'2016-07-28': [55091,2422,1687,529,793,976,663,527,2141,1319,994,2362,40678],
'2016-07-29': [55781,1296,1024,1487,1510,838,577,564,2195,1242,1004,2362,41682],
'2016-07-30': [30837,855,962,693,401,342,285,268,1062,729,637,1346,23257],
'2016-07-31': [29180,1135,973,484,370,268,278,228,1023,766,558,1359,21738],
'2016-08-01': [71307,3472,2730,1394,1109,688,280,491,2692,1901,1333,3077,52140],
'2016-08-02': [64137,2777,2356,1341,757,328,526,631,2540,1540,1168,2520,47653],
'2016-08-03': [61657,2555,2293,1019,385,575,687,633,2363,1503,1198,2245,46201],
'2016-08-04': [56302,2522,1773,512,723,827,705,559,2061,1284,1003,1912,42421],
'2016-08-05': [61800,1574,1126,1546,1475,1014,765,620,2177,1434,1141,2391,46537],
'2016-08-06': [31790,842,876,619,461,363,276,211,1146,783,551,1168,24494],
'2016-08-07': [28217,1047,898,467,373,295,191,158,1104,689,535,1032,21428],
'2016-08-08': [56965,2585,2298,1171,817,400,161,451,2217,1330,1120,1863,42552],
'2016-08-09': [55542,2435,2092,1124,506,188,469,619,2197,1248,1067,1626,41971],
'2016-08-10': [59149,2574,2292,822,243,609,799,672,2313,1489,1147,1645,44544],
'2016-08-11': [60094,2535,1751,376,871,979,720,594,2339,1463,1139,1396,45931],
'2016-08-12': [56834,983,556,1470,1449,869,656,536,2124,1315,1014,1224,44638],
'2016-08-13': [13143,94,362,291,182,134,94,109,497,331,240,287,10522],
'2016-08-14': [35297,1283,1110,604,409,322,331,267,1299,768,645,620,27639],
'2016-08-15': [62862,2715,2528,1228,816,533,319,443,2324,1481,1207,1095,48173],
'2016-08-16': [62627,2524,2255,1204,678,357,497,619,2296,1519,1139,972,48567],
'2016-08-17': [60482,2330,2125,842,446,564,677,556,2054,1391,961,808,47728],
'2016-08-18': [57751,2282,1664,542,702,834,579,487,1989,1274,766,707,45925],
'2016-08-19': [54781,1313,974,1369,1223,769,582,483,1977,1168,691,681,43551],
'2016-08-20': [39910,1011,971,637,441,339,283,339,1499,1287,656,387,32060],
'2016-08-21': [31987,1097,905,482,349,293,274,241,1139,762,467,285,25693],
'2016-08-22': [59472,2497,2275,1128,840,430,281,432,2166,1386,654,520,46863],
'2016-08-23': [59225,2228,1977,1056,572,331,438,583,2014,1222,737,279,47788],
'2016-08-24': [56995,2229,1948,793,378,491,622,471,2085,1150,695,83,46050],
'2016-08-25': [55285,2154,1471,477,692,798,628,480,2016,1004,702,0,44863],
'2016-08-26': [51595,1183,870,1153,1142,708,545,442,1753,858,573,0,42368],
'2016-08-27': [32094,940,835,589,406,312,236,239,1091,544,320,0,26582],
'2016-08-28': [31196,1003,910,519,359,274,216,200,1118,529,253,0,25815],
'2016-08-29': [55299,2378,2124,1072,725,413,212,399,1969,758,457,0,44792],
'2016-08-30': [59021,2294,2011,962,489,250,454,593,1915,846,285,0,48922],
'2016-08-31': [59940,2247,1879,767,379,618,687,561,1967,961,102,0,49772],
'2016-09-01': [54464,2199,1560,478,791,886,654,537,1762,983,0,0,44614],
'2016-09-02': [50809,1179,886,1333,1251,752,536,446,1560,883,0,0,41983],
'2016-09-03': [29897,879,848,630,356,345,226,206,856,417,0,0,25134],
'2016-09-04': [28521,1111,901,429,326,252,255,209,812,373,0,0,23853],
'2016-09-05': [64448,3053,2560,1232,887,605,363,327,1717,822,0,0,52882],
'2016-09-06': [58923,2596,2206,1006,680,438,380,383,1623,412,0,0,49199],
'2016-09-07': [60512,2358,2006,957,587,451,499,482,1675,140,0,0,51357],
'2016-09-08': [53361,2234,1774,610,479,572,446,284,1499,0,0,0,45463],
'2016-09-09': [48481,1897,1233,759,639,518,317,183,1363,0,0,0,41572],
'2016-09-10': [46982,1180,1311,912,654,353,209,175,1275,0,0,0,40913],
'2016-09-11': [28264,925,797,467,291,209,189,163,707,0,0,0,24516],
'2016-09-12': [44153,1582,1488,636,272,220,207,410,1016,0,0,0,38322],
'2016-09-13': [40836,1858,1100,385,269,233,486,585,510,0,0,0,35410],
'2016-09-14': [43588,981,807,444,328,788,870,514,209,0,0,0,38647],
'2016-09-15': [24876,815,652,399,367,342,269,84,0,0,0,0,21948],
'2016-09-16': [23373,718,528,428,403,245,102,0,0,0,0,0,20949],
'2016-09-17': [22717,729,665,457,273,115,0,0,0,0,0,0,20478],
'2016-09-18': [25653,981,832,471,156,0,0,0,0,0,0,0,23213],
'2016-09-19': [57793,2933,2401,625,0,0,0,0,0,0,0,0,51834],
'2016-09-20': [60248,2472,1317,0,0,0,0,0,0,0,0,0,56459],
'2016-09-21': [53548,0,0,0,0,0,0,0,0,0,0,0,53548],
              },
        startDate : "",
        endDate : "",
        dateFormat : "",
        title : "Retention Analysis"};

$.fn.Retention = function (options) {
    var graphTitle = options.title || "Retention Graph";
    var data = options.data || null;

    var container = d3.select(this[0]).append("div")
        .attr("class", "box");

    var header = container.append("div")
        .attr("class", "box-header with-border");
    var title = header.append("p")
        .attr("class", "box-title")
        .text(graphTitle);
    var controls = header.append("div")
        .attr("class", "box-tools");
    var dateRange = controls.append("input")
        .attr("id", "date-range")
        .attr("type", "hidden"); //TODO: implement daterangepicker
    var switchContainer = controls.append("div")
        .attr("class", "switch-field");
    var switchData = ["days", "weeks", "months"];
    var switches = switchContainer.selectAll("span")
        .data(switchData)
        .enter()
        .append("span");
    var radios =  switches.append("input")
        .attr("type", "radio")
        .attr("name", "switch")
        .attr("id", function (d) {
            return d;
        })
        .attr("value", function (d) {
            return d;
        });
    var labelsForSwitches = switches.append("label")
        .attr("for", function (d) {
            return d;
        })
        .text(function (d) {
            return d;
        });

    var body = container.append("div")
        .attr("class", "box-body");

    var table = body.append("table")
        .attr("class", "table table-bordered text-center");

    var headData = ["Date \\ Days", "day0", "day1", "day2", "day3", "day4", "day5", "day6", "day7", "week2", "week3", "month1", "month2", "loss"];

    var tHead = table.append("thead")
        .append("tr")
        .attr("class", "retention-thead")
        .selectAll("td")
        .data(headData)
        .enter()
        .append("td")
        .attr("class", function (d, i) {
            if(i == 0)
                return "retention-date"
            else
                return "days"
        })
        .text(function (d) {
            return d;
        });

    var rowsData = getRows(data);

    var tBody = table.append("tbody");

    var rows = tBody.selectAll("tr")
        .data(rowsData).enter()
        .append("tr");

    var cells = rows.selectAll("td")
        .data(function (row, i) {
            return row;
        }).enter()
        .append("td")
        .attr("class", function (d, i) {
            if(i == 0)
                return "retention-date";
            else
                return "days";
        })
        .attr("style", function (d, i) {
            if(i > 1)
            return "background-color :" + shadeColor("", d);
        })
        .append("div")
        .attr("data-toggle", "tooltip")
        .attr("title", function (d, i) {
            if(i != 0 && i != 1 && d != 0)
            return tooltipData(d, i);
        })
        .text(function (d, i) {
            return d + (i > 1 ? "%" : "");
        });

    $('[data-toggle="tooltip"]').tooltip(); //calling bootstrap tooltip

};
