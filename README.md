# Recommendation

Geliştirilen uygulama, içeriklerin birbine olan benzerliklerini hesaplamayı amaçlamıştır. İçeriklere ait bir kaç farklı parametreye alınarak cosinüs fonksiyonu uygulanarak hesaplamalar yapılmıştır. Uygulama temelde 2'ye ayrılmaktadır. Bunlar içeriklerin benzer listesi ve kullanıcıların özel önerme listesidir. 

İçeriklerin Benzer Listesi

Sistem üzerinde bulunan içeriklerin aşağıdaki parametreleri kullanılarak birbirleri arasındaki cosinüs yakınlığı hesaplarak önerme yapılmaktadır.
![vodforvod](https://user-images.githubusercontent.com/99995225/182096924-ff9f6892-6b48-4f3e-abd0-12d595d792ca.png)


Kullanıcılara Özel Önerme

Kullanıcıların sistem üzerinden yapmış olduğu izlemelere göre içerik listesi önerme işlemi yapılmaktadır. Kullanıların izlemeleri içeriğin tipi ve süresi ile birlikte sistem üzerinde tekrar 0.1-0.5 arasında olacak şekilde hesaplanmaktadır. Hesaplamalar bittikten sonra her hesaplama toplanarak kullanıcı için önerme listesi teslim edilir. 

![vodforuser](https://user-images.githubusercontent.com/99995225/182127001-3b1b2cc2-4905-42f8-aebe-8067f8cce916.png)
