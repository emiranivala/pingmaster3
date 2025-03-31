<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎉 Happy Birthday Rajib! 🎂</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Permanent+Marker&family=Raleway:wght@300&family=Roboto+Mono:wght@500&display=swap');

        body {
            margin: 0;
            padding: 0;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            background: linear-gradient(135deg, #ffafbd, #ffc3a0);
            font-family: 'Raleway', sans-serif;
            overflow: hidden;
            animation: dynamicBackground 5s infinite alternate;
        }

        body::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            backdrop-filter: blur(5px);
            background: rgba(255, 255, 255, 0.2);
            z-index: -1;
        }

        .container {
            text-align: center;
            background: rgba(255, 255, 255, 0.9);
            padding: 30px;
            border-radius: 20px;
            box-shadow: 0 15px 30px rgba(0, 0, 0, 0.2);
            animation: zoomIn 1.5s ease;
        }

        h1 {
            color: #ff6f61;
            font-size: 3rem;
            font-family: 'Permanent Marker', cursive;
            margin-bottom: 10px;
            text-shadow: 2px 2px 5px rgba(0, 0, 0, 0.2);
            position: relative;
            animation: hover 2s infinite;
        }

        p {
            color: #555;
            font-size: 1.5rem;
            margin-bottom: 20px;
            font-weight: bold;
            font-family: 'Roboto Mono', monospace;
        }

        @keyframes zoomIn {
            0% {
                transform: scale(0.5);
                opacity: 0;
            }
            100% {
                transform: scale(1);
                opacity: 1;
            }
        }

        @keyframes dynamicBackground {
            0% {
                background: linear-gradient(135deg, #ffafbd, #ffc3a0);
            }
            50% {
                background: linear-gradient(135deg, #a18cd1, #fbc2eb);
            }
            100% {
                background: linear-gradient(135deg, #ffafbd, #ffc3a0);
            }
        }

        @keyframes hover {
            0% {
                transform: translateY(0);
            }
            50% {
                transform: translateY(-10px);
            }
            100% {
                transform: translateY(0);
            }
        }

        .confetti {
            position: absolute;
            width: 10px;
            height: 10px;
            background-color: #fff;
            border-radius: 50%;
            animation: fall 5s linear infinite;
        }

        .confetti:nth-child(odd) {
            background-color: #ff6f61;
        }

        .confetti:nth-child(even) {
            background-color: #ffd54f;
        }

        @keyframes fall {
            0% {
                transform: translateY(-100vh) rotate(0deg);
                opacity: 1;
            }
            100% {
                transform: translateY(100vh) rotate(360deg);
                opacity: 0;
            }
        }

        .balloons {
            position: absolute;
            display: flex;
            justify-content: center;
            width: 100%;
            top: 0;
            animation: floatBalloons 10s infinite;
        }

        .balloon {
            width: 50px;
            height: 70px;
            background: radial-gradient(circle at bottom, #ff6f61, #ff3d3d);
            border-radius: 50%;
            margin: 0 10px;
            position: relative;
            animation: balloonFloat 5s ease-in-out infinite;
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
        }

        .balloon:nth-child(1) {
            background: url('https://via.placeholder.com/50x70') no-repeat center center;
            background-size: cover;
            animation-duration: 6s;
        }

        .balloon:nth-child(2) {
            background: url('https://via.placeholder.com/50x70') no-repeat center center;
            background-size: cover;
            animation-duration: 4s;
        }

        .balloon:nth-child(3) {
            background: url('https://via.placeholder.com/50x70') no-repeat center center;
            background-size: cover;
            animation-duration: 5s;
        }

        .balloon:nth-child(4) {
            background: url('https://via.placeholder.com/50x70') no-repeat center center;
            background-size: cover;
            animation-duration: 7s;
        }

        .balloon:nth-child(5) {
            background: url('https://via.placeholder.com/50x70') no-repeat center center;
            background-size: cover;
            animation-duration: 6.5s;
        }

        .balloon::after {
            content: '';
            position: absolute;
            bottom: -20px;
            left: 50%;
            width: 2px;
            height: 100px; /* Extend the string to reach the text */
            background: #555;
            transform: translateX(-50%);
        }

        @keyframes floatBalloons {
            0% {
                transform: translateY(0);
            }
            50% {
                transform: translateY(-20px);
            }
            100% {
                transform: translateY(0);
            }
        }

        @keyframes balloonFloat {
            0% {
                transform: translateY(0) rotate(0deg);
            }
            50% {
                transform: translateY(-30px) rotate(10deg);
            }
            100% {
                transform: translateY(0) rotate(-10deg);
            }
        }

        .message {
            display: none;
            margin-top: 20px;
            font-size: 1.5rem;
            font-family: 'Permanent Marker', cursive;
            color: #ff6f61;
            animation: typeEffect 5s steps(25, end), colorChange 2s infinite;
        }

        @keyframes colorChange {
            0% { color: #ff6f61; }
            25% { color: #ffd54f; }
            50% { color: #a18cd1; }
            75% { color: #ff3d3d; }
            100% { color: #ff6f61; }
        }

        @keyframes typeEffect {
            from {
                width: 0;
            }
            to {
                width: 100%;
            }
        }

        .message::after {
            content: '';
            display: inline-block;
            width: 10px;
            height: 2px;
            background: black;
            animation: blinkCursor 0.7s infinite;
        }

        @keyframes blinkCursor {
            0%, 100% {
                opacity: 1;
            }
            50% {
                opacity: 0;
            }
        }

        .meme {
            position: absolute;
            bottom: 10px;
            right: 10px;
            width: 150px;
            height: auto;
        }

    </style>
</head>
<body>
    <div class="balloons">
        <div class="balloon"></div>
        <div class="balloon"></div>
        <div class="balloon"></div>
        <div class="balloon"></div>
        <div class="balloon"></div>
    </div>

    <div class="container">
        <h1>🎂 Happy Birthday, Rajib! 🎉</h1>
        <p>Wishing you a day full of smiles 😄, cakes 🎂, and fun 🎊!</p>
        <p class="message">Okay, that's enough! I want my party now! 🍕🎉</p>
    </div>

    <img class="meme" src="https://via.placeholder.com/150" alt="Meme">

    <!-- Confetti Effect -->
    <script>
        const body = document.body;
        for (let i = 0; i < 100; i++) {
            const confetti = document.createElement('div');
            confetti.classList.add('confetti');
            confetti.style.left = Math.random() * 100 + 'vw';
            confetti.style.animationDelay = Math.random() * 5 + 's';
            body.appendChild(confetti);
        }

        // Show funny message after 5 seconds with typing effect
        const message = document.querySelector('.message');
        setTimeout(() => {
            message.style.display = 'block';
        }, 5000);
    </script>
</body>
</html>
