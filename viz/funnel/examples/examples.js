/* global D3Funnel */

const data = {
	non_deposit: [
        ['流程開始', 17032],
		['送出', 16399],
		['基本資料填寫', 10302],
		['填寫職業及財力證明', 6103],
		['帳單與服務', 4612],
        ['確認申請', 4257],
        ['完成送出', 3794],
        ['列印申請書', 3031],
	],
	deposit: [
        ['流程開始', 6980],
        ['送出', 4166],
        ['送出驗證', 3210],
        ['基本資料填寫', 2704],
        ['填寫職業及財力證明', 1704],
        ['帳單與服務', 1492],
        ['確認申請', 1405],
        ['完成送出', 1146]
	],
	creditcard: [
        ['流程開始', 25726],
        ['送出', 17311],
        ['送出驗證', 14894],
        ['基本資料填寫', 13200],
        ['帳單與服務', 12589],
        ['確認申請', 11712],
        ['完成送出', 10472]
	],
};
const options = {
	minHeight1: [
		data.non_deposit, {
			block: {
				dynamicHeight: true,
				minHeight: 20,
			},
		},
	],
    minHeight2: [
        data.deposit, {
            block: {
                dynamicHeight: true,
                minHeight: 20,
            },
        },
    ],
    minHeight3: [
        data.creditcard, {
            block: {
                dynamicHeight: true,
                minHeight: 20,
            },
        },
    ],
};

const chart = new D3Funnel('#funnel');
const picker = document.getElementById('picker');

picker.addEventListener('change', () => {
	const index = picker.value;

	// Reverse the dataset if the isInverted option is present
	// Otherwise, just use the regular data
	if ('isInverted' in options[index][1]) {
		chart.draw(options[index][0].reverse(), options[index][1]);
	} else {
		chart.draw(options[index][0], options[index][1]);
	}
});

// Trigger change event for initial render
picker.dispatchEvent(new CustomEvent('change'));
