/* sub accordian 접근성 */

jQuery(function($){		
		
		var menu_v = $('div.menu_v');
		var sItem = menu_v.find('>ul>li');
		var ssItem = menu_v.find('>ul>li>ul>li');
		var lastEvent = null;
		
		sItem.find('>ul').css('display','none');
		menu_v.find('>ul>li>ul>li[class=active]').parents('li').attr('class','active');
		menu_v.find('>ul>li[class=active]').find('>ul').css('display','block');

		function menu_vToggle(event){
			var t = $(this);
			
			if (this == lastEvent) return false;
			lastEvent = this;
			setTimeout(function(){ lastEvent=null }, 200);
			
			if (t.next('ul').is(':hidden')) {
				sItem.find('>ul').slideUp(100);
				t.next('ul').slideDown(100);
			} else if(!t.next('ul').length) {
				sItem.find('>ul').slideUp(100);
			} 
			//else {
				//t.next('ul').slideUp(100);
			//}
			
			if (t.parent('li').hasClass('active')){
				t.parent('li').removeClass('active');
			} else {
				sItem.removeClass('active');
				t.parent('li').addClass('active');
			}
		}
		sItem.find('>a').click(menu_vToggle).focus(menu_vToggle);
		
		function subMenuActive(){
			ssItem.removeClass('active');
			$(this).parent(ssItem).addClass('active');
		}; 
		ssItem.find('>a').click(subMenuActive).focus(subMenuActive);	
	});


/* left 접기펴기 */

function lnbClose() { 
	document.getElementById('leftLnb').style.display  ='none';
	document.getElementById('container').style.padding  ='0';
	document.getElementById('btnOpen').style.display  ='none';
	document.getElementById('btnClose').style.display  ='block';		
}
function lnbOpen() { 
	document.getElementById('leftLnb').style.display  ='block';
	document.getElementById('container').style.paddingLeft  ='230px';
	document.getElementById('btnOpen').style.display  ='block';
	document.getElementById('btnClose').style.display  ='none';		
}


