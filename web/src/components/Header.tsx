import Link from "next/link";
import React from "react";

const Header = () => {
  return (
    <header className="flex justify-between items-center p-5">
      <div className="text-4xl font-bold text-purdue-boilermakerGold font-logo">
        <Link href={"/"}>FRESCO</Link>
      </div>
      <nav className="flex text-lg font-semibold space-x-8 text-purdue-boilermakerGold">
        <Link href="/">Home</Link>
        <Link href="/about">About</Link>
        <Link href="/team">Team</Link>
        {/* <a href="/News">News</a> */}
      </nav>
    </header>
  );
};

export default Header;
